//revive:disable:comments-density reason: retry-oriented control flow is clearer than filler comments here.
package resources

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

const stepFunctionRetryBaseDelay = 200 * time.Millisecond
const stepFunctionMaxAttempts = 5
const stepFunctionLastRetryAttempt = stepFunctionMaxAttempts - 1

// collectStepFunctionRuns lists recent executions and retries transient throttling.
func collectStepFunctionRuns(ctx context.Context, svc *sfn.Client, stateMachineARN string, since time.Time, maxResults int) ([]Run, error) {
	input := &sfn.ListExecutionsInput{StateMachineArn: aws.String(stateMachineARN), MaxResults: safeInt32(maxResults)}
	p := sfn.NewListExecutionsPaginator(svc, input)
	runs := make([]Run, 0)
	for p.HasMorePages() {
		var (
			page *sfn.ListExecutionsOutput
			err  error
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
			ex := page.Executions[executionIndex]
			start := aws.ToTime(ex.StartDate)
			if start.Before(since) {
				continue
			}
			run := Run{RunID: aws.ToString(ex.Name), Status: string(ex.Status), StartAt: formatRFC3339UTC(start), SourceService: "stepfunctions"}
			if ex.StopDate != nil {
				end := aws.ToTime(ex.StopDate)
				run.EndAt = formatRFC3339UTC(end)
				dur := int64(end.Sub(start).Seconds())
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

// isThrottlingError matches throttling responses without depending on SDK error types.
func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "throttling") || strings.Contains(msg, "rate exceeded")
}
