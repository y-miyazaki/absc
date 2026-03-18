//revive:disable:comments-density reason: utility helpers are straightforward and extra comments would be repetitive.
package resources

import (
	"strconv"
	"strings"
	"time"
)

const (
	buildSlotsFieldCount = 2
	closeSuffix          = ")"
	buildSlotsDayHours   = 24
	buildSlotsDayMinutes = buildSlotsDayHours * 60
	cronHourLimit        = 23
	cronMinuteLimit      = 59
	maxInt32Value        = 1<<31 - 1
	parseCronSplitParts  = 2
	rateDayDefaultSlot   = 0
	arnServiceIndex      = 2
	arnSplitParts        = 6
	colonSeparator       = ":"
	sdkMinParts          = 2
	sdkSplitParts        = 3
	slotsPerHour         = 6
	slotsPerRateMinute   = 10
	schedulerSDKMarker   = ":aws-sdk:"
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
	slots := make([]int, slotsPerDay)
	e := strings.TrimSpace(expr)
	if strings.HasPrefix(e, "cron(") && strings.HasSuffix(e, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(e, "cron("), closeSuffix)
		fields := strings.Fields(inside)
		if len(fields) >= buildSlotsFieldCount {
			mins := parseCronField(fields[0], 0, cronMinuteLimit)
			hours := parseCronField(fields[1], 0, cronHourLimit)
			for _, h := range hours {
				for _, m := range mins {
					idx := h*slotsPerHour + (m / slotsPerRateMinute)
					if idx >= 0 && idx < slotsPerDay {
						slots[idx] = 1
					}
				}
			}
		}
		return slots
	}
	if strings.HasPrefix(e, "rate(") && strings.HasSuffix(e, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(e, "rate("), closeSuffix)
		fields := strings.Fields(inside)
		if len(fields) >= buildSlotsFieldCount {
			n, err := strconv.Atoi(fields[0])
			if err == nil && n > 0 {
				unit := strings.ToLower(fields[1])
				switch {
				case strings.HasPrefix(unit, "minute"):
					for m := 0; m < buildSlotsDayMinutes; m += n {
						slots[m/slotsPerRateMinute] = 1
					}
				case strings.HasPrefix(unit, "hour"):
					for h := 0; h < buildSlotsDayHours; h += n {
						slots[h*slotsPerHour] = 1
					}
				default:
					slots[rateDayDefaultSlot] = 1
				}
			}
		}
	}
	return slots
}

func parseCronField(field string, minValue, maxValue int) []int {
	f := strings.TrimSpace(field)
	// Expand all values when the field is a wildcard.
	if f == "*" || f == "?" {
		vals := make([]int, 0, maxValue-minValue+1)
		for i := minValue; i <= maxValue; i++ {
			vals = append(vals, i)
		}
		return vals
	}
	result := make(map[int]struct{})
	for _, part := range strings.Split(f, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		if strings.HasPrefix(p, "*/") {
			n, err := strconv.Atoi(strings.TrimPrefix(p, "*/"))
			if err != nil || n <= 0 {
				continue
			}
			for i := minValue; i <= maxValue; i += n {
				result[i] = struct{}{}
			}
			continue
		}
		if strings.Contains(p, "-") {
			sp := strings.SplitN(p, "-", parseCronSplitParts)
			start, e1 := strconv.Atoi(sp[0])
			end, e2 := strconv.Atoi(sp[1])
			if e1 != nil || e2 != nil {
				continue
			}
			if start < minValue {
				start = minValue
			}
			if end > maxValue {
				end = maxValue
			}
			for i := start; i <= end; i++ {
				result[i] = struct{}{}
			}
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			continue
		}
		if v >= minValue && v <= maxValue {
			result[v] = struct{}{}
		}
	}
	vals := make([]int, 0, len(result))
	for v := range result {
		vals = append(vals, v)
	}
	// Fall back to the full supported range when no explicit values were parsed.
	if len(vals) == 0 {
		for i := minValue; i <= maxValue; i++ {
			vals = append(vals, i)
		}
	}
	return vals
}

func safeInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > maxInt32Value {
		return maxInt32Value
	}
	return int32(value)
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
	if arn == "" {
		return ""
	}
	parts := strings.Split(arn, "/")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}
	parts = strings.Split(arn, ":")
	return parts[len(parts)-1]
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

func fromMillis(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

func fromMillisPtr(v *int64) time.Time {
	if v == nil {
		return time.Time{}
	}
	return fromMillis(*v)
}

// formatRFC3339UTC normalizes collected service timestamps to canonical UTC.
// User-facing timezone conversion is handled later in exporter.BuildOutput.
func formatRFC3339UTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// formatRFC3339NanoUTC normalizes auxiliary identifiers based on timestamp to UTC.
func formatRFC3339NanoUTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
