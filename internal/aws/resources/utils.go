//revive:disable:comments-density reason: utility helpers are straightforward and extra comments would be repetitive.
package resources

import (
	"strings"
	"time"

	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	arnServiceIndex    = 2
	arnSplitParts      = 6
	colonSeparator     = ":"
	sdkMinParts        = 2
	sdkSplitParts      = 3
	timelineSlotMinute = 10
	schedulerSDKMarker = ":aws-sdk:"
)

var serviceLabelByARNService = map[string]string{
	"batch":    "Batch",
	"ec2":      "EC2",
	"ecs":      "ECS",
	"events":   "EventBridge",
	"glue":     "Glue",
	"lambda":   "Lambda",
	"rds":      "RDS",
	"redshift": "Redshift",
	"states":   "Step Functions",
}

var serviceLabelBySDKService = map[string]string{
	"batch":    "Batch",
	"ec2":      "EC2",
	"ecs":      "ECS",
	"glue":     "Glue",
	"lambda":   "Lambda",
	"rds":      "RDS",
	"redshift": "Redshift",
	"sfn":      "Step Functions",
}

// buildSlots maps cron or rate expressions into a fixed per-day slot timeline.
func buildSlots(expr string) []int {
	return helpers.BuildDailySlots(expr, timelineSlotMinute)
}

//nolint:unused // Transitional compatibility helper for legacy collectors.
func parseCronField(field string, minValue, maxValue int) []int {
	return helpers.ParseCronField(field, minValue, maxValue)
}

//nolint:unused // Transitional compatibility helper for legacy collectors.
func safeInt32(value int) int32 {
	return helpers.SafeInt32(value)
}

//revive:disable-next-line:flag-parameter reason: batch parameters are part of the target classification input.
func detectTargetKind(arn string, batchParametersPresent bool) string {
	s := strings.ToLower(arn)
	if strings.Contains(s, ":aws-sdk:sfn:startexecution") {
		return "stepfunctions"
	}
	if strings.Contains(s, ":aws-sdk:batch:submitjob") {
		return "batch"
	}
	if strings.Contains(s, ":aws-sdk:ec2:") {
		return "ec2"
	}
	if strings.Contains(s, ":aws-sdk:rds:") {
		return "rds"
	}
	if strings.Contains(s, ":aws-sdk:lambda:") {
		return "lambda"
	}
	if strings.Contains(s, ":aws-sdk:glue:") {
		return "glue"
	}
	if strings.Contains(s, ":aws-sdk:ecs:") {
		return "ecs"
	}
	if strings.Contains(s, ":states:") && strings.Contains(s, ":statemachine:") {
		return "stepfunctions"
	}
	if batchParametersPresent || strings.Contains(s, ":batch:") {
		return "batch"
	}
	if strings.Contains(s, ":glue:") {
		return "glue"
	}
	if strings.Contains(s, ":ecs:") {
		return "ecs"
	}
	if strings.Contains(s, ":lambda:") {
		return "lambda"
	}
	return "other"
}

func detectTargetService(arn string) string {
	v := strings.TrimSpace(arn)
	if v == "" {
		return "Other"
	}

	lower := strings.ToLower(v)
	if idx := strings.Index(lower, schedulerSDKMarker); idx >= 0 {
		sdkPart := lower[idx+len(schedulerSDKMarker):]
		sdkParts := strings.SplitN(sdkPart, colonSeparator, sdkSplitParts)
		if len(sdkParts) >= 1 {
			if label, ok := serviceLabelBySDKService[sdkParts[0]]; ok {
				return label
			}
		}
		return "Other"
	}

	arnParts := strings.SplitN(lower, colonSeparator, arnSplitParts)
	if len(arnParts) > arnServiceIndex {
		if label, ok := serviceLabelByARNService[arnParts[arnServiceIndex]]; ok {
			return label
		}
	}

	return "Other"
}

func resourceNameFromARN(arn string) string {
	return helpers.ResourceNameFromARN(arn)
}

// detectTargetAction returns a raw service:action label from EventBridge Scheduler
// aws-sdk target ARNs (e.g. arn:aws:scheduler:::aws-sdk:sfn:startExecution -> sfn:startExecution).
// Returns empty string for direct-resource ARNs (EventBridge Rule targets).
func detectTargetAction(arn string) string {
	lowerARN := strings.ToLower(arn)
	idx := strings.Index(lowerARN, schedulerSDKMarker)
	if idx < 0 {
		return ""
	}

	sdkPart := arn[idx+len(schedulerSDKMarker):]
	parts := strings.SplitN(sdkPart, colonSeparator, sdkSplitParts)
	if len(parts) < sdkMinParts {
		return ""
	}

	service := strings.TrimSpace(parts[0])
	action := strings.TrimSpace(parts[1])
	if service == "" || action == "" {
		return ""
	}

	return service + ":" + action
}

//nolint:unused // Transitional compatibility helper for legacy collectors.
func fromMillis(v int64) time.Time {
	return helpers.FromMillis(v)
}

//nolint:unused // Transitional compatibility helper for legacy collectors.
func fromMillisPtr(v *int64) time.Time {
	return helpers.FromMillisPtr(v)
}

// formatRFC3339UTC normalizes collected service timestamps to canonical UTC.
// User-facing timezone conversion is handled later in exporter.BuildOutput.
//
//nolint:unused // Transitional compatibility helper for legacy collectors.
func formatRFC3339UTC(t time.Time) string {
	return helpers.FormatRFC3339UTC(t)
}

// formatRFC3339NanoUTC normalizes auxiliary identifiers based on timestamp to UTC.
//
//nolint:unused // Transitional compatibility helper for legacy collectors.
func formatRFC3339NanoUTC(t time.Time) string {
	return helpers.FormatRFC3339NanoUTC(t)
}
