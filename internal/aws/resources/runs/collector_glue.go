// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: collector code is intentionally linear and concise.
package runs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

type glueCollector struct {
	caches *runCollectorCaches
	svc    *glue.Client
}

func newGlueCollector(svc *glue.Client, caches *runCollectorCaches) *glueCollector {
	return &glueCollector{caches: caches, svc: svc}
}

func (*glueCollector) Service() string { return "glue" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *glueCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = schedule
	_ = runJobName
	_ = hints
	description := fmt.Sprintf("Glue job=%s", helpers.ResourceNameFromARN(targetARN))
	runs, err := getCachedRunsForCollector(c.caches, c, targetARN, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect glue runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func (c *glueCollector) collectRuns(ctx context.Context, jobNameOrARN string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	jobName := jobNameOrARN
	if strings.Contains(jobNameOrARN, ":") {
		jobName = helpers.ResourceNameFromARN(jobNameOrARN)
	}
	if jobName == "" {
		return make([]resourcescore.Run, 0), nil
	}

	runs := make([]resourcescore.Run, 0, maxResults)
	var nextToken *string
	for len(runs) < maxResults {
		pageSize := remainingPageSize(maxResults, len(runs), glueGetJobRunsPageSizeMax)
		out, err := c.svc.GetJobRuns(ctx, &glue.GetJobRunsInput{JobName: aws.String(jobName), MaxResults: &pageSize, NextToken: nextToken})
		if err != nil {
			return nil, fmt.Errorf("get glue job runs for %s: %w", jobName, err)
		}
		for jobRunIndex := range out.JobRuns {
			jobRun := out.JobRuns[jobRunIndex]
			if jobRun.StartedOn == nil || jobRun.StartedOn.Before(since) || (!until.IsZero() && jobRun.StartedOn.After(until)) {
				continue
			}
			run := resourcescore.Run{RunID: aws.ToString(jobRun.Id), Status: string(jobRun.JobRunState), StartAt: helpers.FormatRFC3339UTC(*jobRun.StartedOn), SourceService: "glue"}
			if jobRun.CompletedOn != nil {
				run.EndAt = helpers.FormatRFC3339UTC(*jobRun.CompletedOn)
				duration := int64(jobRun.CompletedOn.Sub(*jobRun.StartedOn).Seconds())
				run.DurationSec = &duration
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				break
			}
		}
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	return runs, nil
}
