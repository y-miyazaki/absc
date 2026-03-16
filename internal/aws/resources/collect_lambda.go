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

var lambdaDurationPattern = regexp.MustCompile(`Duration:\s*([0-9]+(?:\.[0-9]+)?)\s*ms`)

// collectLambdaRuns extracts recent Lambda invocations from CloudWatch Logs REPORT lines.
func collectLambdaRuns(ctx context.Context, svc *cloudwatchlogs.Client, functionTarget string, since time.Time, maxResults int) ([]Run, error) {
	functionName := lambdaFunctionName(functionTarget)
	if functionName == "" {
		return []Run{}, nil
	}

	logGroupName := "/aws/lambda/" + functionName
	startTimeMillis := since.UnixMilli()
	input := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		StartTime:     aws.Int64(startTimeMillis),
		FilterPattern: aws.String("\"REPORT RequestId\""),
	}

	p := cloudwatchlogs.NewFilterLogEventsPaginator(svc, input)
	runs := make([]Run, 0, maxResults)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("filter lambda logs for %s: %w", functionName, err)
		}
		for _, ev := range page.Events {
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
				Status:        "COMPLETED",
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

func lambdaFunctionName(functionTarget string) string {
	t := strings.TrimSpace(functionTarget)
	if t == "" {
		return ""
	}
	if strings.Contains(t, ":function:") {
		parts := strings.SplitN(t, ":function:", 2)
		if len(parts) == 2 {
			nameWithQualifier := parts[1]
			return strings.SplitN(nameWithQualifier, ":", 2)[0]
		}
	}
	if strings.Contains(t, ":") {
		return resourceNameFromARN(t)
	}
	return t
}

func lambdaDurationSec(message string) int64 {
	m := lambdaDurationPattern.FindStringSubmatch(message)
	if len(m) < 2 {
		return 0
	}
	ms, err := strconv.ParseFloat(m[1], 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return int64(math.Ceil(ms / 1000.0))
}

func eventIDOrTime(eventID *string, t time.Time) string {
	if eventID != nil && *eventID != "" {
		return *eventID
	}
	return formatRFC3339NanoUTC(t)
}
