package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchtypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
)

// collectBatchRuns queries Batch job history by queue and optional job name.
// Batch results are paginated per job status, so this collector walks each state.
// The function stops early once maxResults is satisfied.
func collectBatchRuns(ctx context.Context, svc *batch.Client, targetARN, jobName string, since, until time.Time, maxResults int) ([]Run, error) {
	// Resolve the queue name from either full ARN or plain queue identifier.
	queueName := resourceNameFromARN(targetARN)
	if queueName == "" {
		queueName = targetARN
	}
	statuses := []batchtypes.JobStatus{batchtypes.JobStatusSubmitted, batchtypes.JobStatusPending, batchtypes.JobStatusRunnable, batchtypes.JobStatusStarting, batchtypes.JobStatusRunning, batchtypes.JobStatusSucceeded, batchtypes.JobStatusFailed}
	// Keep the original ordering so newer in-flight states are checked first.
	runs := make([]Run, 0)
	for _, status := range statuses {
		if len(runs) >= maxResults {
			break
		}
		maxResults32 := safeInt32(maxResults)
		// Request each page with the caller's cap so pagination can stop early.
		input := &batch.ListJobsInput{JobQueue: aws.String(queueName), JobStatus: status, MaxResults: &maxResults32}
		p := batch.NewListJobsPaginator(svc, input)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("list jobs for %s: %w", queueName, err)
			}
			// Convert Batch summaries into the shared Run shape.
			for jobIndex := range page.JobSummaryList {
				j := page.JobSummaryList[jobIndex]
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
				if !until.IsZero() && start.After(until) {
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
