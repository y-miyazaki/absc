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
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	stepFunctionLastRetryAttempt = stepFunctionMaxAttempts - 1
	stepFunctionMaxAttempts      = 5
	stepFunctionRetryBaseDelay   = 200 * time.Millisecond
)

type stepFunctionsCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
	svc    *sfn.Client
}

func newStepFunctionsCollector(svc *sfn.Client, ctSvc *cloudtrail.Client, caches *runCollectorCaches) *stepFunctionsCollector {
	return &stepFunctionsCollector{caches: caches, ctSvc: ctSvc, svc: svc}
}

func (*stepFunctionsCollector) Service() string { return "stepfunctions" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *stepFunctionsCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = runJobName
	_ = hints
	description := fmt.Sprintf("Step Function state machine=%s", helpers.ResourceNameFromARN(targetARN))
	runs, err := getCachedRunsForCollector(c.caches, c, targetARN, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, schedule.TargetAction, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect step function runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func (c *stepFunctionsCollector) collectRuns(ctx context.Context, targetAction, stateMachineARN string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	if !isMeasurableAction(c.Service(), targetAction) {
		runs, err := c.collectCloudTrailRuns(ctx, targetAction, stateMachineARN, since, until, maxResults)
		if err != nil {
			return nil, fmt.Errorf("collect non-measurable stepfunctions action via cloudtrail: %w", err)
		}
		return runs, nil
	}

	input := &sfn.ListExecutionsInput{StateMachineArn: aws.String(stateMachineARN), MaxResults: pageSizeForLimit(maxResults, stepFunctionsListExecutionsPageSizeMax)}
	p := sfn.NewListExecutionsPaginator(c.svc, input)
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
			if attempt == stepFunctionLastRetryAttempt || !c.isThrottlingError(err) {
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

func (c *stepFunctionsCollector) collectCloudTrailRuns(ctx context.Context, targetAction, stateMachineARN string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	runs, err := collectCloudTrailRunsForResources(ctx, c.ctSvc, targetAction, c.cloudTrailResourceIDs(stateMachineARN), since, until, maxResults, c.caches, c.runsFromCloudTrailEvent)
	if err != nil {
		return nil, fmt.Errorf("collect stepfunctions cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (*stepFunctionsCollector) runsFromCloudTrailEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	return genericCloudTrailRunsFromEvent(
		event,
		since,
		stepFunctionsCloudTrailRequestResourceKeys,
	)
}

func (*stepFunctionsCollector) cloudTrailResourceIDs(stateMachineARN string) []string {
	trimmed := strings.TrimSpace(stateMachineARN)
	if trimmed == "" {
		return nil
	}
	ids := appendUniqueTrimmedResourceIDs(nil, trimmed)
	return appendResourceNameFromARN(ids, trimmed)
}

func (*stepFunctionsCollector) isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "throttling") || strings.Contains(message, "rate exceeded")
}
