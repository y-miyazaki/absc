package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// collectECSRuns returns stopped and running ECS tasks for the given cluster ARN.
// The ECS API retains stopped task data for approximately 1 hour.
func collectECSRuns(ctx context.Context, svc *ecs.Client, clusterARN string, since time.Time, maxResults int) ([]Run, error) {
	var taskARNs []string

	for _, desired := range []ecstypes.DesiredStatus{ecstypes.DesiredStatusStopped, ecstypes.DesiredStatusRunning} {
		var nextToken *string
		for {
			out, err := svc.ListTasks(ctx, &ecs.ListTasksInput{
				Cluster:       aws.String(clusterARN),
				DesiredStatus: desired,
				NextToken:     nextToken,
			})
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
		return []Run{}, nil
	}

	runs := make([]Run, 0, len(taskARNs))
	for i := 0; i < len(taskARNs); i += 100 {
		end := i + 100
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
		for _, t := range desc.Tasks {
			if t.StartedAt == nil {
				continue
			}
			if t.StartedAt.Before(since) {
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
