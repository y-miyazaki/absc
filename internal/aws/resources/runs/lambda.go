// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: parser-focused collector code is intentionally compact.
package runs

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
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	lambdaFilterPatternPlatformReport = "\"platform.report\""
	lambdaFilterPatternReportLine     = "\"REPORT RequestId\""
	lambdaFloatBitSize                = 64
	lambdaPlatformReportType          = "platform.report"
	lambdaSplitParts                  = 2
	lambdaStatusCompleted             = "COMPLETED"
	lambdaStatusFailed                = "FAILED"
)

var lambdaDurationPattern = regexp.MustCompile(`Duration:\s*(\d+(?:\.\d+)?)\s*ms`)
var lambdaErrorTypePattern = regexp.MustCompile(`Error Type:\s*([A-Za-z0-9._-]+)`)
var lambdaStatusPattern = regexp.MustCompile(`Status:\s*([A-Za-z_]+)`)

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

func collectLambdaRuns(ctx context.Context, svc *cloudwatchlogs.Client, functionTarget string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	functionName := lambdaFunctionName(functionTarget)
	if functionName == "" {
		return make([]resourcescore.Run, 0), nil
	}

	runs := make([]resourcescore.Run, 0, maxResults)
	seen := make(map[string]struct{}, maxResults)
	for _, filterPattern := range []string{lambdaFilterPatternReportLine, lambdaFilterPatternPlatformReport} {
		matchedRuns, err := collectLambdaRunsWithPattern(ctx, svc, functionName, since, until, maxResults, filterPattern)
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

	slices.SortStableFunc(runs, func(left, right resourcescore.Run) int {
		return lambdaRunSortTime(&right).Compare(lambdaRunSortTime(&left))
	})
	if len(runs) > maxResults {
		return runs[:maxResults], nil
	}
	return runs, nil
}

func collectLambdaRunsWithPattern(ctx context.Context, svc *cloudwatchlogs.Client, functionName string, since, until time.Time, maxResults int, filterPattern string) ([]resourcescore.Run, error) {
	pageSize := pageSizeForLimit(maxResults, cloudWatchLogsFilterEventsPageSizeMax)
	input := &cloudwatchlogs.FilterLogEventsInput{LogGroupName: aws.String("/aws/lambda/" + functionName), StartTime: aws.Int64(since.UnixMilli()), FilterPattern: aws.String(filterPattern), Limit: &pageSize}
	if !until.IsZero() {
		input.EndTime = aws.Int64(until.UnixMilli())
	}
	p := cloudwatchlogs.NewFilterLogEventsPaginator(svc, input)
	runs := make([]resourcescore.Run, 0, maxResults)
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

func lambdaRunFromLogEvent(event cloudwatchlogstypes.FilteredLogEvent, since time.Time) (resourcescore.Run, bool) {
	if event.Timestamp == nil || event.Message == nil {
		return resourcescore.Run{}, false
	}
	end := helpers.FromMillis(*event.Timestamp)
	if end.IsZero() || end.Before(since) {
		return resourcescore.Run{}, false
	}

	durationSec := lambdaDurationSec(*event.Message)
	start := end
	if durationSec > 0 {
		start = end.Add(-time.Duration(durationSec) * time.Second)
	}

	run := resourcescore.Run{RunID: eventIDOrTime(event.EventId, end), Status: lambdaRunStatus(*event.Message), StartAt: helpers.FormatRFC3339UTC(start), EndAt: helpers.FormatRFC3339UTC(end), SourceService: "lambda"}
	if durationSec > 0 {
		duration := durationSec
		run.DurationSec = &duration
	}
	return run, true
}

func lambdaFunctionName(functionTarget string) string {
	trimmed := strings.TrimSpace(functionTarget)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, ":function:") {
		parts := strings.SplitN(trimmed, ":function:", lambdaSplitParts)
		if len(parts) == lambdaSplitParts {
			return strings.SplitN(parts[1], ":", lambdaSplitParts)[0]
		}
	}
	if strings.Contains(trimmed, ":") {
		return helpers.ResourceNameFromARN(trimmed)
	}
	return trimmed
}

func lambdaDurationSec(message string) int64 {
	if report, ok := parseLambdaPlatformReport(message); ok {
		if report.Record.Metrics.DurationMs <= 0 {
			return 0
		}
		return int64(math.Ceil(report.Record.Metrics.DurationMs / 1000.0))
	}
	match := lambdaDurationPattern.FindStringSubmatch(message)
	if len(match) < lambdaSplitParts {
		return 0
	}
	milliseconds, err := strconv.ParseFloat(match[1], lambdaFloatBitSize)
	if err != nil || milliseconds <= 0 {
		return 0
	}
	return int64(math.Ceil(milliseconds / 1000.0))
}

func lambdaRunStatus(message string) string {
	if lambdaRunStatusDetail(message) == lambdaStatusCompleted {
		return lambdaStatusCompleted
	}
	return lambdaStatusFailed
}

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
	if match := lambdaStatusPattern.FindStringSubmatch(message); len(match) == lambdaSplitParts {
		errorType := ""
		if errorMatch := lambdaErrorTypePattern.FindStringSubmatch(message); len(errorMatch) == lambdaSplitParts {
			errorType = errorMatch[1]
		}
		return lambdaStatusDetailFromFields(match[1], errorType)
	}
	return lambdaStatusCompleted
}

func lambdaRunSortTime(run *resourcescore.Run) time.Time {
	if parsed, err := time.Parse(time.RFC3339, run.EndAt); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, run.StartAt); err == nil {
		return parsed
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

func eventIDOrTime(eventID *string, timestamp time.Time) string {
	if eventID != nil && *eventID != "" {
		return *eventID
	}
	return helpers.FormatRFC3339NanoUTC(timestamp)
}
