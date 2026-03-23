// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: collector code is intentionally linear and concise.
package runs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

func collectBatchRuns(ctx context.Context, svc *batch.Client, targetARN, jobName string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	queueName := helpers.ResourceNameFromARN(targetARN)
	if queueName == "" {
		queueName = targetARN
	}
	statuses := []batchtypes.JobStatus{batchtypes.JobStatusSubmitted, batchtypes.JobStatusPending, batchtypes.JobStatusRunnable, batchtypes.JobStatusStarting, batchtypes.JobStatusRunning, batchtypes.JobStatusSucceeded, batchtypes.JobStatusFailed}
	runs := make([]resourcescore.Run, 0)
	pageSize := pageSizeForLimit(maxResults, batchListJobsPageSizeMax)
	for _, status := range statuses {
		if len(runs) >= maxResults {
			break
		}
		input := &batch.ListJobsInput{JobQueue: aws.String(queueName), JobStatus: status, MaxResults: &pageSize}
		p := batch.NewListJobsPaginator(svc, input)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("list jobs for %s: %w", queueName, err)
			}
			for jobIndex := range page.JobSummaryList {
				job := page.JobSummaryList[jobIndex]
				if jobName != "" && aws.ToString(job.JobName) != jobName {
					continue
				}
				start := helpers.FromMillisPtr(job.StartedAt)
				if start.IsZero() {
					start = helpers.FromMillisPtr(job.CreatedAt)
				}
				if start.IsZero() || start.Before(since) || (!until.IsZero() && start.After(until)) {
					continue
				}
				run := resourcescore.Run{RunID: aws.ToString(job.JobId), Status: string(job.Status), StartAt: helpers.FormatRFC3339UTC(start), SourceService: "batch"}
				stop := helpers.FromMillisPtr(job.StoppedAt)
				if !stop.IsZero() {
					run.EndAt = helpers.FormatRFC3339UTC(stop)
					duration := int64(stop.Sub(start).Seconds())
					run.DurationSec = &duration
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
