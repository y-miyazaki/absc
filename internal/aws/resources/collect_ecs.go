package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

const ecsDescribeTasksBatchSize = 100

// collectECSRuns returns ECS executions for the given cluster and target hints.
// It prefers ECS task details and backfills older windows from CloudTrail RunTask events.
func collectECSRuns(ctx context.Context, ecsSvc *ecs.Client, ctSvc *cloudtrail.Client, clusterARN string, hints runTargetHints, since, until time.Time, maxResults int, caches *runCollectorCaches) ([]Run, error) {
	ecsRuns, ecsErr := collectECSRunsFromAPI(ctx, ecsSvc, clusterARN, hints.ecsTaskDefinitionARN, hints.ecsStartedBy, since, until, maxResults)
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

// collectECSRunsFromAPI returns stopped and running ECS tasks for the given cluster ARN.
// Optional task definition and startedBy filters avoid mixing unrelated tasks.
// The ECS API retains stopped task data for approximately 1 hour.
func collectECSRunsFromAPI(ctx context.Context, svc *ecs.Client, clusterARN, taskDefinitionARN, startedBy string, since, until time.Time, maxResults int) ([]Run, error) {
	// Query both running and stopped tasks because recent executions may still be active.
	var taskARNs []string

	// ECS splits task discovery by desired status, so both views are queried explicitly.
	for _, desired := range []ecstypes.DesiredStatus{ecstypes.DesiredStatusStopped, ecstypes.DesiredStatusRunning} {
		var nextToken *string
		for {
			// ListTasks returns lightweight ARNs which are described in a second step.
			out, err := svc.ListTasks(ctx, &ecs.ListTasksInput{
				Cluster:       aws.String(clusterARN),
				DesiredStatus: desired,
				NextToken:     nextToken,
			})
			if err != nil {
				return nil, fmt.Errorf("list ecs tasks for cluster %s: %w", clusterARN, err)
			}
			taskARNs = append(taskARNs, out.TaskArns...)
			// A small amount of over-fetch keeps the second-stage describe calls bounded.
			if out.NextToken == nil || len(taskARNs) >= maxResults*2 {
				break
			}
			nextToken = out.NextToken
		}
	}

	if len(taskARNs) == 0 {
		return make([]Run, 0), nil
	}

	// Describe tasks in batches to stay within ECS API limits.
	runs := make([]Run, 0, len(taskARNs))
	for i := 0; i < len(taskARNs); i += ecsDescribeTasksBatchSize {
		end := i + ecsDescribeTasksBatchSize
		if end > len(taskARNs) {
			end = len(taskARNs)
		}
		desc, err := svc.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(clusterARN),
			Tasks:   taskARNs[i:end],
		})
		if err != nil {
			return nil, fmt.Errorf("describe ecs tasks for cluster %s: %w", clusterARN, err)
		}
		// Convert task state into the shared Run shape.
		for taskIndex := range desc.Tasks {
			t := desc.Tasks[taskIndex]
			if taskDefinitionARN != "" && aws.ToString(t.TaskDefinitionArn) != taskDefinitionARN {
				continue
			}
			if startedBy != "" && aws.ToString(t.StartedBy) != startedBy {
				continue
			}
			// Tasks without StartedAt have not transitioned into an execution window yet.
			if t.StartedAt == nil {
				continue
			}
			if t.StartedAt.Before(since) {
				continue
			}
			if !until.IsZero() && t.StartedAt.After(until) {
				continue
			}
			run := Run{
				RunID:         resourceNameFromARN(aws.ToString(t.TaskArn)),
				Status:        aws.ToString(t.LastStatus),
				StartAt:       formatRFC3339UTC(*t.StartedAt),
				SourceService: "ecs",
			}
			if t.StoppedAt != nil && !t.StoppedAt.IsZero() {
				run.EndAt = formatRFC3339UTC(*t.StoppedAt)
				dur := int64(t.StoppedAt.Sub(*t.StartedAt).Seconds())
				run.DurationSec = &dur
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				return runs, nil
			}
		}
	}
	return runs, nil
}
