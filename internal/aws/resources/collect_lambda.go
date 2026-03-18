//revive:disable:comments-density reason: parser logic is straightforward and additional comments would add noise.
package resources

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

var lambdaDurationPattern = regexp.MustCompile(`Duration:\s*(\d+(?:\.\d+)?)\s*ms`)
var lambdaErrorTypePattern = regexp.MustCompile(`Error Type:\s*([A-Za-z0-9._-]+)`)
var lambdaStatusPattern = regexp.MustCompile(`Status:\s*([A-Za-z_]+)`)

const lambdaSplitParts = 2
const lambdaFloatBitSize = 64
const lambdaStatusCompleted = "COMPLETED"
const lambdaStatusFailed = "FAILED"

// collectLambdaRuns extracts recent Lambda invocations from CloudWatch Logs REPORT lines.
func collectLambdaRuns(ctx context.Context, svc *cloudwatchlogs.Client, functionTarget string, since time.Time, maxResults int) ([]Run, error) {
	// Resolve the effective function name from either ARN or plain name inputs.
	functionName := lambdaFunctionName(functionTarget)
	if functionName == "" {
		return make([]Run, 0), nil
	}

	logGroupName := "/aws/lambda/" + functionName
	startTimeMs := since.UnixMilli()
	// Filter for REPORT lines because they contain duration and request identifiers.
	input := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		StartTime:     aws.Int64(startTimeMs),
		FilterPattern: aws.String("\"REPORT RequestId\""),
	}

	// REPORT log lines contain billed duration close to the end timestamp.
	p := cloudwatchlogs.NewFilterLogEventsPaginator(svc, input)
	runs := make([]Run, 0, maxResults)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("filter lambda logs for %s: %w", functionName, err)
		}
		// Convert each REPORT event into a completed invocation record.
		for eventIndex := range page.Events {
			ev := page.Events[eventIndex]
			if ev.Timestamp == nil || ev.Message == nil {
				continue
			}
			end := fromMillis(*ev.Timestamp)
			if end.IsZero() || end.Before(since) {
				continue
			}
			durSec := lambdaDurationSec(*ev.Message)
			start := end
			if durSec > 0 {
				start = end.Add(-time.Duration(durSec) * time.Second)
			}

			run := Run{
				RunID:         eventIDOrTime(ev.EventId, end),
				Status:        lambdaRunStatus(*ev.Message),
				StartAt:       formatRFC3339UTC(start),
				EndAt:         formatRFC3339UTC(end),
				SourceService: "lambda",
			}
			if durSec > 0 {
				d := durSec
				run.DurationSec = &d
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				return runs, nil
			}
		}
	}
	return runs, nil
}

// lambdaFunctionName normalizes supported Lambda target formats to a function name.
func lambdaFunctionName(functionTarget string) string {
	// Event targets can be plain names, Lambda ARNs, or qualified Lambda ARNs.
	t := strings.TrimSpace(functionTarget)
	if t == "" {
		return ""
	}
	if strings.Contains(t, ":function:") {
		parts := strings.SplitN(t, ":function:", lambdaSplitParts)
		if len(parts) == lambdaSplitParts {
			nameWithQualifier := parts[1]
			return strings.SplitN(nameWithQualifier, ":", lambdaSplitParts)[0]
		}
	}
	if strings.Contains(t, ":") {
		return resourceNameFromARN(t)
	}
	return t
}

// lambdaDurationSec extracts the reported duration and rounds it to whole seconds.
func lambdaDurationSec(message string) int64 {
	// REPORT lines contain duration in milliseconds, which is rounded up to seconds.
	m := lambdaDurationPattern.FindStringSubmatch(message)
	if len(m) < lambdaSplitParts {
		return 0
	}
	ms, err := strconv.ParseFloat(m[1], lambdaFloatBitSize)
	if err != nil || ms <= 0 {
		return 0
	}
	return int64(math.Ceil(ms / 1000.0))
}

// lambdaRunStatus infers invocation status from REPORT lines.
func lambdaRunStatus(message string) string {
	if lambdaRunStatusDetail(message) == lambdaStatusCompleted {
		return lambdaStatusCompleted
	}
	return lambdaStatusFailed
}

// lambdaRunStatusDetail keeps detailed failure reasons for future UI improvements.
func lambdaRunStatusDetail(message string) string {
	lower := strings.ToLower(message)

	if strings.Contains(lower, "task timed out") || strings.Contains(lower, "status: timeout") {
		return "TIMED_OUT"
	}
	if strings.Contains(lower, "outofmemory") || strings.Contains(lower, "out of memory") {
		return "OUT_OF_MEMORY"
	}

	if m := lambdaStatusPattern.FindStringSubmatch(message); len(m) == lambdaSplitParts {
		switch strings.ToUpper(strings.TrimSpace(m[1])) {
		case "SUCCESS":
			return lambdaStatusCompleted
		case "TIMEOUT":
			return "TIMED_OUT"
		case "ERROR":
			if em := lambdaErrorTypePattern.FindStringSubmatch(message); len(em) == lambdaSplitParts {
				errType := strings.ToUpper(strings.TrimSpace(em[1]))
				if strings.Contains(errType, "OUTOFMEMORY") {
					return "OUT_OF_MEMORY"
				}
			}
			return lambdaStatusFailed
		}
	}

	return lambdaStatusCompleted
}

// eventIDOrTime prefers the CloudWatch event id and falls back to a timestamp key.
func eventIDOrTime(eventID *string, t time.Time) string {
	if eventID != nil && *eventID != "" {
		return *eventID
	}
	return formatRFC3339NanoUTC(t)
}
