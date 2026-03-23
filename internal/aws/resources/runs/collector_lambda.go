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
	lambdaPatternFetchMultiplier      = 2
	lambdaPlatformReportType          = "platform.report"
	lambdaSplitParts                  = 2
	lambdaStatusCompleted             = "COMPLETED"
	lambdaStatusFailed                = "FAILED"
)

var lambdaDurationPattern = regexp.MustCompile(`Duration:\s*(\d+(?:\.\d+)?)\s*ms`)
var lambdaErrorTypePattern = regexp.MustCompile(`Error Type:\s*([A-Za-z0-9._-]+)`)
var lambdaStatusPattern = regexp.MustCompile(`Status:\s*([A-Za-z_]+)`)

type lambdaCollector struct {
	caches *runCollectorCaches
	svc    *cloudwatchlogs.Client
}

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

func newLambdaCollector(svc *cloudwatchlogs.Client, caches *runCollectorCaches) *lambdaCollector {
	return &lambdaCollector{caches: caches, svc: svc}
}

func (*lambdaCollector) Service() string { return "lambda" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *lambdaCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = schedule
	_ = runJobName
	_ = hints
	description := fmt.Sprintf("Lambda function=%s", c.functionName(targetARN))
	runs, err := getCachedRunsForCollector(c.caches, c, targetARN, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect lambda runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func (c *lambdaCollector) collectRuns(ctx context.Context, functionTarget string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	functionName := c.functionName(functionTarget)
	if functionName == "" {
		return make([]resourcescore.Run, 0), nil
	}

	// Use a map to deduplicate runs by RequestID across both filter patterns.
	// RequestID is stable and reliable (Lambda Request IDs are unique per invocation).
	runsByID := make(map[string]resourcescore.Run)
	for _, filterPattern := range []string{lambdaFilterPatternReportLine, lambdaFilterPatternPlatformReport} {
		matchedRuns, err := c.collectRunsWithPattern(ctx, functionName, since, until, maxResults*lambdaPatternFetchMultiplier, filterPattern)
		if err != nil {
			return nil, fmt.Errorf("collect lambda runs with filter %s for %s: %w", filterPattern, functionName, err)
		}
		for runIndex := range matchedRuns {
			run := matchedRuns[runIndex]
			// Use RunID (RequestID) as the deduplication key; if not present, time-based fallback is already in RunID
			runsByID[run.RunID] = run
		}
	}

	runs := make([]resourcescore.Run, 0, len(runsByID))
	for runID := range runsByID {
		runs = append(runs, runsByID[runID])
	}

	slices.SortStableFunc(runs, func(left, right resourcescore.Run) int {
		return c.runSortTime(&right).Compare(c.runSortTime(&left))
	})
	if len(runs) > maxResults {
		return runs[:maxResults], nil
	}
	return runs, nil
}

func (c *lambdaCollector) collectRunsWithPattern(ctx context.Context, functionName string, since, until time.Time, maxResults int, filterPattern string) ([]resourcescore.Run, error) {
	pageSize := pageSizeForLimit(maxResults, cloudWatchLogsFilterEventsPageSizeMax)
	input := &cloudwatchlogs.FilterLogEventsInput{LogGroupName: aws.String("/aws/lambda/" + functionName), StartTime: aws.Int64(since.UnixMilli()), FilterPattern: aws.String(filterPattern), Limit: &pageSize}
	if !until.IsZero() {
		input.EndTime = aws.Int64(until.UnixMilli())
	}
	p := cloudwatchlogs.NewFilterLogEventsPaginator(c.svc, input)
	runs := make([]resourcescore.Run, 0, maxResults)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("filter lambda logs for %s: %w", functionName, err)
		}
		for eventIndex := range page.Events {
			run, ok := c.runFromLogEvent(page.Events[eventIndex], since)
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

func (c *lambdaCollector) runFromLogEvent(event cloudwatchlogstypes.FilteredLogEvent, since time.Time) (resourcescore.Run, bool) {
	if event.Timestamp == nil || event.Message == nil {
		return resourcescore.Run{}, false
	}
	end := helpers.FromMillis(*event.Timestamp)
	if end.IsZero() || end.Before(since) {
		return resourcescore.Run{}, false
	}

	durationSec := c.durationSec(*event.Message)
	start := end
	if durationSec > 0 {
		start = end.Add(-time.Duration(durationSec) * time.Second)
	}

	run := resourcescore.Run{RunID: c.eventIDOrTime(event.EventId, end), Status: c.runStatus(*event.Message), StartAt: helpers.FormatRFC3339UTC(start), EndAt: helpers.FormatRFC3339UTC(end), SourceService: "lambda"}
	if durationSec > 0 {
		duration := durationSec
		run.DurationSec = &duration
	}
	return run, true
}

func (*lambdaCollector) functionName(functionTarget string) string {
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

func (c *lambdaCollector) durationSec(message string) int64 {
	if report, ok := c.parsePlatformReport(message); ok {
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

func (c *lambdaCollector) runStatus(message string) string {
	if c.runStatusDetail(message) == lambdaStatusCompleted {
		return lambdaStatusCompleted
	}
	return lambdaStatusFailed
}

func (c *lambdaCollector) runStatusDetail(message string) string {
	if report, ok := c.parsePlatformReport(message); ok {
		return c.statusDetailFromFields(report.Record.Status, report.Record.ErrorType)
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
		return c.statusDetailFromFields(match[1], errorType)
	}
	return lambdaStatusCompleted
}

func (*lambdaCollector) runSortTime(run *resourcescore.Run) time.Time {
	if parsed, err := time.Parse(time.RFC3339, run.EndAt); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, run.StartAt); err == nil {
		return parsed
	}
	return time.Time{}
}

func (*lambdaCollector) statusDetailFromFields(status, errorType string) string {
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

func (*lambdaCollector) parsePlatformReport(message string) (lambdaPlatformReport, bool) {
	var report lambdaPlatformReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(message)), &report); err != nil {
		return lambdaPlatformReport{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(report.Type), lambdaPlatformReportType) {
		return lambdaPlatformReport{}, false
	}
	return report, true
}

func (*lambdaCollector) eventIDOrTime(eventID *string, timestamp time.Time) string {
	if eventID != nil && *eventID != "" {
		return *eventID
	}
	return helpers.FormatRFC3339NanoUTC(timestamp)
}
