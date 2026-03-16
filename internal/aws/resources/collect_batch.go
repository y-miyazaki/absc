package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
)

func collectBatchRuns(ctx context.Context, svc *batch.Client, targetARN, jobName string, since time.Time, maxResults int) ([]Run, error) {
	queueName := resourceNameFromARN(targetARN)
	if queueName == "" {
		queueName = targetARN
	}
	statuses := []batchtypes.JobStatus{batchtypes.JobStatusSubmitted, batchtypes.JobStatusPending, batchtypes.JobStatusRunnable, batchtypes.JobStatusStarting, batchtypes.JobStatusRunning, batchtypes.JobStatusSucceeded, batchtypes.JobStatusFailed}
	runs := make([]Run, 0)
	for _, status := range statuses {
		if len(runs) >= maxResults {
			break
		}
		max := int32(maxResults)
		input := &batch.ListJobsInput{JobQueue: aws.String(queueName), JobStatus: status, MaxResults: &max}
		p := batch.NewListJobsPaginator(svc, input)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("list jobs for %s: %w", queueName, err)
			}
			for _, j := range page.JobSummaryList {
				if jobName != "" && aws.ToString(j.JobName) != jobName {
					continue
				}
				start := fromMillisPtr(j.StartedAt)
				if start.IsZero() {
					start = fromMillisPtr(j.CreatedAt)
				}
				if start.IsZero() || start.Before(since) {
					continue
				}
				run := Run{RunID: aws.ToString(j.JobId), Status: string(j.Status), StartAt: formatRFC3339UTC(start), SourceService: "batch"}
				stop := fromMillisPtr(j.StoppedAt)
				if !stop.IsZero() {
					run.EndAt = formatRFC3339UTC(stop)
					dur := int64(stop.Sub(start).Seconds())
					run.DurationSec = &dur
				}
				runs = append(runs, run)
				if len(runs) >= maxResults {
					break
				}
			}
			if len(runs) >= maxResults {
				break
			}
		}
	}
	return runs, nil
}
