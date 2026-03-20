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
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	stepFunctionLastRetryAttempt = stepFunctionMaxAttempts - 1
	stepFunctionMaxAttempts      = 5
	stepFunctionRetryBaseDelay   = 200 * time.Millisecond
)

func collectGlueRuns(ctx context.Context, svc *glue.Client, jobNameOrARN string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	jobName := jobNameOrARN
	if strings.Contains(jobNameOrARN, ":") {
		jobName = helpers.ResourceNameFromARN(jobNameOrARN)
	}
	if jobName == "" {
		return make([]resourcescore.Run, 0), nil
	}

	maxRes := helpers.SafeInt32(maxResults)
	if maxRes == 0 {
		maxRes = helpers.SafeInt32(defaultMaxResults)
	}

	runs := make([]resourcescore.Run, 0, maxResults)
	var nextToken *string
	for len(runs) < maxResults {
		out, err := svc.GetJobRuns(ctx, &glue.GetJobRunsInput{JobName: aws.String(jobName), MaxResults: &maxRes, NextToken: nextToken})
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

func collectStepFunctionRuns(ctx context.Context, svc *sfn.Client, stateMachineARN string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	input := &sfn.ListExecutionsInput{StateMachineArn: aws.String(stateMachineARN), MaxResults: helpers.SafeInt32(maxResults)}
	p := sfn.NewListExecutionsPaginator(svc, input)
	runs := make([]resourcescore.Run, 0)
	for p.HasMorePages() {
		var (
			err  error
			page *sfn.ListExecutionsOutput
		)
		for attempt := 0; attempt < stepFunctionMaxAttempts; attempt++ {
			page, err = p.NextPage(ctx)
			if err == nil {
				break
			}
			if attempt == stepFunctionLastRetryAttempt || !isThrottlingError(err) {
				return nil, fmt.Errorf("list executions for %s: %w", stateMachineARN, err)
			}
			backoff := stepFunctionRetryBaseDelay * time.Duration(1<<attempt)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, fmt.Errorf("list executions for %s: %w", stateMachineARN, ctx.Err())
			case <-timer.C:
			}
		}
		if err != nil {
			return nil, fmt.Errorf("list executions for %s: %w", stateMachineARN, err)
		}
		for executionIndex := range page.Executions {
			execution := page.Executions[executionIndex]
			start := aws.ToTime(execution.StartDate)
			if start.Before(since) || (!until.IsZero() && start.After(until)) {
				continue
			}
			run := resourcescore.Run{RunID: aws.ToString(execution.Name), Status: string(execution.Status), StartAt: helpers.FormatRFC3339UTC(start), SourceService: "stepfunctions"}
			if execution.StopDate != nil {
				end := aws.ToTime(execution.StopDate)
				run.EndAt = helpers.FormatRFC3339UTC(end)
				duration := int64(end.Sub(start).Seconds())
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

func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "throttling") || strings.Contains(message, "rate exceeded")
}
