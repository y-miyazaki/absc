//revive:disable:comments-density reason: parser logic is straightforward and additional comments would add noise.
package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatchlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

const lambdaFilterPatternPlatformReport = "\"platform.report\""
const lambdaFilterPatternReportLine = "\"REPORT RequestId\""
const lambdaPlatformReportType = "platform.report"

var lambdaDurationPattern = regexp.MustCompile(`Duration:\s*(\d+(?:\.\d+)?)\s*ms`)
var lambdaErrorTypePattern = regexp.MustCompile(`Error Type:\s*([A-Za-z0-9._-]+)`)
var lambdaStatusPattern = regexp.MustCompile(`Status:\s*([A-Za-z_]+)`)

const lambdaSplitParts = 2
const lambdaFloatBitSize = 64
const lambdaStatusCompleted = "COMPLETED"
const lambdaStatusFailed = "FAILED"

type lambdaPlatformReport struct {
	Type   string                     `json:"type"`
	Record lambdaPlatformReportRecord `json:"record"`
}

type lambdaPlatformReportMetrics struct {
	DurationMs float64 `json:"durationMs"`
}

type lambdaPlatformReportRecord struct {
	ErrorType string                      `json:"errorType"`
	RequestID string                      `json:"requestId"`
	Status    string                      `json:"status"`
	Metrics   lambdaPlatformReportMetrics `json:"metrics"`
}

// collectLambdaRuns extracts recent Lambda invocations from CloudWatch Logs REPORT lines.
func collectLambdaRuns(ctx context.Context, svc *cloudwatchlogs.Client, functionTarget string, since time.Time, maxResults int) ([]Run, error) {
	// Resolve the effective function name from either ARN or plain name inputs.
	functionName := lambdaFunctionName(functionTarget)
	if functionName == "" {
		return make([]Run, 0), nil
	}

	runs := make([]Run, 0, maxResults)
	seen := make(map[string]struct{}, maxResults)
	for _, filterPattern := range []string{lambdaFilterPatternReportLine, lambdaFilterPatternPlatformReport} {
		matchedRuns, err := collectLambdaRunsWithPattern(ctx, svc, functionName, since, maxResults, filterPattern)
		if err != nil {
			return nil, fmt.Errorf("collect lambda runs with filter %s for %s: %w", filterPattern, functionName, err)
		}
		for runIndex := range matchedRuns {
			run := matchedRuns[runIndex]
			key := run.RunID + "|" + run.StartAt + "|" + run.EndAt
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			runs = append(runs, run)
		}
	}

	slices.SortStableFunc(runs, func(left, right Run) int {
		return lambdaRunSortTime(&right).Compare(lambdaRunSortTime(&left))
	})
	if len(runs) > maxResults {
		return runs[:maxResults], nil
	}
	return runs, nil
}

func collectLambdaRunsWithPattern(ctx context.Context, svc *cloudwatchlogs.Client, functionName string, since time.Time, maxResults int, filterPattern string) ([]Run, error) {
	logGroupName := "/aws/lambda/" + functionName
	input := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		StartTime:     aws.Int64(since.UnixMilli()),
		FilterPattern: aws.String(filterPattern),
	}

	p := cloudwatchlogs.NewFilterLogEventsPaginator(svc, input)
	runs := make([]Run, 0, maxResults)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("filter lambda logs for %s: %w", functionName, err)
		}
		for eventIndex := range page.Events {
			run, ok := lambdaRunFromLogEvent(page.Events[eventIndex], since)
			if !ok {
				continue
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				return runs, nil
			}
		}
	}
	return runs, nil
}

func lambdaRunFromLogEvent(ev cloudwatchlogstypes.FilteredLogEvent, since time.Time) (Run, bool) {
	if ev.Timestamp == nil || ev.Message == nil {
		return Run{}, false
	}
	end := fromMillis(*ev.Timestamp)
	if end.IsZero() || end.Before(since) {
		return Run{}, false
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
	return run, true
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
	if report, ok := parseLambdaPlatformReport(message); ok {
		if report.Record.Metrics.DurationMs <= 0 {
			return 0
		}
		return int64(math.Ceil(report.Record.Metrics.DurationMs / 1000.0))
	}

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
	if report, ok := parseLambdaPlatformReport(message); ok {
		return lambdaStatusDetailFromFields(report.Record.Status, report.Record.ErrorType)
	}

	lower := strings.ToLower(message)

	if strings.Contains(lower, "task timed out") || strings.Contains(lower, "status: timeout") {
		return "TIMED_OUT"
	}
	if strings.Contains(lower, "outofmemory") || strings.Contains(lower, "out of memory") {
		return "OUT_OF_MEMORY"
	}

	if m := lambdaStatusPattern.FindStringSubmatch(message); len(m) == lambdaSplitParts {
		errorType := ""
		if em := lambdaErrorTypePattern.FindStringSubmatch(message); len(em) == lambdaSplitParts {
			errorType = em[1]
		}
		return lambdaStatusDetailFromFields(m[1], errorType)
	}

	return lambdaStatusCompleted
}

func lambdaRunSortTime(run *Run) time.Time {
	if t, err := time.Parse(time.RFC3339, run.EndAt); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, run.StartAt); err == nil {
		return t
	}
	return time.Time{}
}

func lambdaStatusDetailFromFields(status, errorType string) string {
	normalizedStatus := strings.ToUpper(strings.TrimSpace(status))
	normalizedErrorType := strings.ToUpper(strings.TrimSpace(errorType))

	if strings.Contains(normalizedErrorType, "OUTOFMEMORY") {
		return "OUT_OF_MEMORY"
	}

	switch normalizedStatus {
	case "TIMEOUT", "TIMED_OUT":
		return "TIMED_OUT"
	case "ERROR", "FAILURE", "FAILED":
		return lambdaStatusFailed
	}

	return lambdaStatusCompleted
}

func parseLambdaPlatformReport(message string) (lambdaPlatformReport, bool) {
	var report lambdaPlatformReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(message)), &report); err != nil {
		return lambdaPlatformReport{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(report.Type), lambdaPlatformReportType) {
		return lambdaPlatformReport{}, false
	}
	return report, true
}

// eventIDOrTime prefers the CloudWatch event id and falls back to a timestamp key.
func eventIDOrTime(eventID *string, t time.Time) string {
	if eventID != nil && *eventID != "" {
		return *eventID
	}
	return formatRFC3339NanoUTC(t)
}
