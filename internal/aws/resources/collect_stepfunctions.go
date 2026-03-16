package resources

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

func collectStepFunctionRuns(ctx context.Context, svc *sfn.Client, stateMachineARN string, since time.Time, maxResults int) ([]Run, error) {
	input := &sfn.ListExecutionsInput{StateMachineArn: aws.String(stateMachineARN), MaxResults: int32(maxResults)}
	p := sfn.NewListExecutionsPaginator(svc, input)
	runs := make([]Run, 0)
	for p.HasMorePages() {
		var (
			page *sfn.ListExecutionsOutput
			err  error
		)
		for attempt := 0; attempt < 5; attempt++ {
			page, err = p.NextPage(ctx)
			if err == nil {
				break
			}
			if !isThrottlingError(err) || attempt == 4 {
				return nil, fmt.Errorf("list executions for %s: %w", stateMachineARN, err)
			}
			backoff := time.Duration(200*(1<<attempt)) * time.Millisecond
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
		for _, ex := range page.Executions {
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

func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "throttling") || strings.Contains(msg, "rate exceeded")
}
