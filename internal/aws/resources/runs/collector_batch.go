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
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const batchCloudTrailResourceIDsCapacity = 3

type batchCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
	svc    *batch.Client
}

func newBatchCollector(svc *batch.Client, ctSvc *cloudtrail.Client, caches *runCollectorCaches) *batchCollector {
	return &batchCollector{caches: caches, ctSvc: ctSvc, svc: svc}
}

func (*batchCollector) Service() string { return "batch" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *batchCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = hints
	cacheKey := targetARN + cacheKeySeparator + runJobName
	description := fmt.Sprintf("batch job queue=%s job=%s", helpers.ResourceNameFromARN(targetARN), runJobName)
	runs, err := getCachedRunsForCollector(c.caches, c, cacheKey, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, schedule.TargetAction, targetARN, runJobName, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect batch runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func (c *batchCollector) collectRuns(ctx context.Context, targetAction, targetARN, jobName string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	if !isMeasurableAction(c.Service(), targetAction) {
		runs, err := c.collectCloudTrailRuns(ctx, targetAction, targetARN, jobName, since, until, maxResults)
		if err != nil {
			return nil, fmt.Errorf("collect non-measurable batch action via cloudtrail: %w", err)
		}
		return runs, nil
	}

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
		p := batch.NewListJobsPaginator(c.svc, input)
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

func (c *batchCollector) collectCloudTrailRuns(ctx context.Context, targetAction, targetARN, jobName string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	runs, err := collectCloudTrailRunsForResources(ctx, c.ctSvc, targetAction, c.cloudTrailResourceIDs(targetARN, jobName), since, until, maxResults, c.caches, c.runsFromCloudTrailEvent)
	if err != nil {
		return nil, fmt.Errorf("collect batch cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (*batchCollector) runsFromCloudTrailEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	return genericCloudTrailRunsFromEvent(
		event,
		since,
		batchCloudTrailRequestResourceKeys,
	)
}

func (*batchCollector) cloudTrailResourceIDs(targetARN, jobName string) []string {
	ids := make([]string, 0, batchCloudTrailResourceIDsCapacity)
	ids = appendUniqueTrimmedResourceIDs(ids, targetARN)
	ids = appendResourceNameFromARN(ids, targetARN)
	return appendUniqueTrimmedResourceIDs(ids, jobName)
}
