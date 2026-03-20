//revive:disable:comments-density reason: cron parsing helpers are dense but mechanically structured.
package resources

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	cronCloseSuffix        = ")"
	cronFieldCount         = 6
	cronSearchYears        = 5
	dayHours               = 24
	minuteHoursMultiplier  = 60
	rateExpressionMinParts = 2
)

func computeSchedulerNextInvocation(detail *scheduler.GetScheduleOutput, nowUTC time.Time) string {
	if detail == nil {
		return ""
	}
	if detail.State != types.ScheduleStateEnabled {
		return ""
	}

	expr := strings.TrimSpace(aws.ToString(detail.ScheduleExpression))
	if expr == "" {
		return ""
	}

	startAt := time.Time{}
	if detail.StartDate != nil {
		startAt = detail.StartDate.UTC()
	}
	endAt := time.Time{}
	if detail.EndDate != nil {
		endAt = detail.EndDate.UTC()
	}

	if strings.HasPrefix(expr, "at(") && strings.HasSuffix(expr, ")") {
		t, ok := parseSchedulerAtExpression(expr, aws.ToString(detail.ScheduleExpressionTimezone))
		if !ok {
			return ""
		}
		candidate := t.UTC()
		if candidate.Before(nowUTC) {
			return ""
		}
		if !startAt.IsZero() && candidate.Before(startAt) {
			candidate = startAt
		}
		if !endAt.IsZero() && candidate.After(endAt) {
			return ""
		}
		return formatRFC3339UTC(candidate)
	}

	if strings.HasPrefix(expr, "rate(") && strings.HasSuffix(expr, ")") {
		candidate, ok := computeRateNextInvocation(expr, detail.CreationDate, startAt, endAt, nowUTC)
		if !ok {
			return ""
		}
		return formatRFC3339UTC(candidate)
	}

	if strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")") {
		candidate, ok := computeCronNextInvocation(expr, aws.ToString(detail.ScheduleExpressionTimezone), startAt, endAt, nowUTC)
		if !ok {
			return ""
		}
		return formatRFC3339UTC(candidate)
	}

	return ""
}

func computeCronNextInvocation(expr, timezone string, startAt, endAt, nowUTC time.Time) (time.Time, bool) {
	inside := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "cron("), cronCloseSuffix)
	fields := strings.Fields(inside)
	if len(fields) != cronFieldCount {
		return time.Time{}, false
	}

	loc := scheduleExpressionLocation(timezone)
	searchStart := nowUTC
	if !startAt.IsZero() && startAt.After(searchStart) {
		searchStart = startAt
	}

	candidate := searchStart.In(loc).Truncate(time.Minute).Add(time.Minute)
	deadline := candidate.AddDate(cronSearchYears, 0, 0)
	if !endAt.IsZero() {
		endLocal := endAt.In(loc)
		if endLocal.Before(deadline) {
			deadline = endLocal
		}
	}

	for !candidate.After(deadline) {
		if matchAWSCronExpression(fields, candidate) {
			utc := candidate.UTC()
			if !endAt.IsZero() && utc.After(endAt) {
				return time.Time{}, false
			}
			return utc, true
		}
		candidate = candidate.Add(time.Minute)
	}

	return time.Time{}, false
}

func computeRateNextInvocation(expr string, creationDate *time.Time, startAt, endAt, nowUTC time.Time) (time.Time, bool) {
	inside := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "rate("), cronCloseSuffix)
	parts := strings.Fields(inside)
	if len(parts) < rateExpressionMinParts {
		return time.Time{}, false
	}

	n, err := time.ParseDuration(parts[0] + "m")
	if err != nil {
		return time.Time{}, false
	}

	unit := strings.ToLower(parts[1])
	switch {
	case strings.HasPrefix(unit, "minute"):
		// Keep n as minutes.
	case strings.HasPrefix(unit, "hour"):
		n *= minuteHoursMultiplier
	case strings.HasPrefix(unit, "day"):
		n *= dayHours * minuteHoursMultiplier
	default:
		return time.Time{}, false
	}

	base := nowUTC
	if creationDate != nil {
		base = creationDate.UTC()
	}
	if !startAt.IsZero() {
		base = startAt
	}

	candidate := base
	if candidate.Before(nowUTC) {
		delta := nowUTC.Sub(candidate)
		stepCount := int(delta/n) + 1
		candidate = candidate.Add(time.Duration(stepCount) * n)
	}

	if !endAt.IsZero() && candidate.After(endAt) {
		return time.Time{}, false
	}
	return candidate, true
}

func matchAWSCronExpression(fields []string, t time.Time) bool {
	return helpers.MatchAWSCronExpression(fields, t)
}

func parseSchedulerAtExpression(expr, timezone string) (time.Time, bool) {
	raw := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "at("), cronCloseSuffix)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}

	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}

	loc := scheduleExpressionLocation(timezone)
	if t, err := time.ParseInLocation("2006-01-02T15:04:05", raw, loc); err == nil {
		return t, true
	}

	return time.Time{}, false
}

func scheduleExpressionLocation(timezone string) *time.Location {
	return helpers.LoadLocationOrUTC(timezone)
}
