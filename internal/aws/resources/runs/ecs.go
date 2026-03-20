// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: collector code is intentionally linear and concise.
package runs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const ecsDescribeTasksBatchSize = 100

func collectECSRuns(ctx context.Context, ecsSvc *ecs.Client, ctSvc *cloudtrail.Client, clusterARN string, hints TargetHints, since, until time.Time, maxResults int, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	ecsRuns, ecsErr := collectECSRunsFromAPI(ctx, ecsSvc, clusterARN, hints.ECSTaskDefinitionARN, hints.ECSStartedBy, since, until, maxResults)
	if !ecsCloudTrailRequired(since, time.Now().UTC()) {
		if ecsErr != nil {
			return nil, fmt.Errorf("collect ecs runs from api: %w", ecsErr)
		}
		return ecsRuns, nil
	}

	cloudTrailRuns, cloudTrailErr := collectECSCloudTrailRuns(ctx, ctSvc, since, caches)
	filteredCloudTrailRuns := filterECSCloudTrailRuns(cloudTrailRuns, clusterARN, hints, maxResults)
	mergedRuns := mergeECSRuns(ecsRuns, filteredCloudTrailRuns, maxResults)

	switch {
	case ecsErr == nil && cloudTrailErr == nil:
		return mergedRuns, nil
	case ecsErr == nil && len(mergedRuns) > 0:
		return mergedRuns, nil
	case cloudTrailErr == nil && len(mergedRuns) > 0:
		return mergedRuns, nil
	case ecsErr != nil && cloudTrailErr != nil:
		return nil, fmt.Errorf("collect ecs runs from api and cloudtrail: %w; %w", ecsErr, cloudTrailErr)
	case ecsErr != nil:
		return nil, fmt.Errorf("collect ecs runs from api: %w", ecsErr)
	default:
		return nil, fmt.Errorf("collect ecs runs from cloudtrail: %w", cloudTrailErr)
	}
}

func collectECSRunsFromAPI(ctx context.Context, svc *ecs.Client, clusterARN, taskDefinitionARN, startedBy string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	var taskARNs []string
	for _, desired := range []ecstypes.DesiredStatus{ecstypes.DesiredStatusStopped, ecstypes.DesiredStatusRunning} {
		var nextToken *string
		for {
			out, err := svc.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String(clusterARN), DesiredStatus: desired, NextToken: nextToken})
			if err != nil {
				return nil, fmt.Errorf("list ecs tasks for cluster %s: %w", clusterARN, err)
			}
			taskARNs = append(taskARNs, out.TaskArns...)
			if out.NextToken == nil || len(taskARNs) >= maxResults*2 {
				break
			}
			nextToken = out.NextToken
		}
	}
	if len(taskARNs) == 0 {
		return make([]resourcescore.Run, 0), nil
	}

	runs := make([]resourcescore.Run, 0, len(taskARNs))
	for i := 0; i < len(taskARNs); i += ecsDescribeTasksBatchSize {
		end := i + ecsDescribeTasksBatchSize
		if end > len(taskARNs) {
			end = len(taskARNs)
		}
		desc, err := svc.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(clusterARN), Tasks: taskARNs[i:end]})
		if err != nil {
			return nil, fmt.Errorf("describe ecs tasks for cluster %s: %w", clusterARN, err)
		}
		for taskIndex := range desc.Tasks {
			task := desc.Tasks[taskIndex]
			if taskDefinitionARN != "" && aws.ToString(task.TaskDefinitionArn) != taskDefinitionARN {
				continue
			}
			if startedBy != "" && aws.ToString(task.StartedBy) != startedBy {
				continue
			}
			if task.StartedAt == nil || task.StartedAt.Before(since) || (!until.IsZero() && task.StartedAt.After(until)) {
				continue
			}
			run := resourcescore.Run{RunID: helpers.ResourceNameFromARN(aws.ToString(task.TaskArn)), Status: aws.ToString(task.LastStatus), StartAt: helpers.FormatRFC3339UTC(*task.StartedAt), SourceService: "ecs"}
			if task.StoppedAt != nil && !task.StoppedAt.IsZero() {
				run.EndAt = helpers.FormatRFC3339UTC(*task.StoppedAt)
				duration := int64(task.StoppedAt.Sub(*task.StartedAt).Seconds())
				run.DurationSec = &duration
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				return runs, nil
			}
		}
	}
	return runs, nil
}
