package resources

import (
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

var cronDayAliases = map[string]int{
	"SUN": 1,
	"MON": 2,
	"TUE": 3,
	"WED": 4,
	"THU": 5,
	"FRI": 6,
	"SAT": 7,
}

var cronMonthAliases = map[string]int{
	"JAN": 1,
	"FEB": 2,
	"MAR": 3,
	"APR": 4,
	"MAY": 5,
	"JUN": 6,
	"JUL": 7,
	"AUG": 8,
	"SEP": 9,
	"OCT": 10,
	"NOV": 11,
	"DEC": 12,
}

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
	inside := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "cron("), ")")
	fields := strings.Fields(inside)
	if len(fields) != 6 {
		return time.Time{}, false
	}

	loc := scheduleExpressionLocation(timezone)
	searchStart := nowUTC
	if !startAt.IsZero() && startAt.After(searchStart) {
		searchStart = startAt
	}

	candidate := searchStart.In(loc).Truncate(time.Minute).Add(time.Minute)
	deadline := candidate.AddDate(5, 0, 0)
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
	inside := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "rate("), ")")
	parts := strings.Fields(inside)
	if len(parts) < 2 {
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
		n *= 60
	case strings.HasPrefix(unit, "day"):
		n *= 24 * 60
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
		steps := (delta / n) + 1
		candidate = candidate.Add(steps * n)
	}

	if !endAt.IsZero() && candidate.After(endAt) {
		return time.Time{}, false
	}
	return candidate, true
}

func matchAWSCronExpression(fields []string, t time.Time) bool {
	if len(fields) != 6 {
		return false
	}

	minute := t.Minute()
	hour := t.Hour()
	dom := t.Day()
	month := int(t.Month())
	dow := int(t.Weekday())
	if dow == 0 {
		dow = 1
	} else {
		dow++
	}
	year := t.Year()

	if !matchCronField(fields[0], minute, 0, 59, nil) {
		return false
	}
	if !matchCronField(fields[1], hour, 0, 23, nil) {
		return false
	}
	if !matchCronField(fields[3], month, 1, 12, cronMonthAliases) {
		return false
	}
	if !matchCronField(fields[5], year, 1970, 2199, nil) {
		return false
	}

	domField := strings.TrimSpace(fields[2])
	dowField := strings.TrimSpace(fields[4])
	domMatch := matchCronField(domField, dom, 1, 31, nil)
	dowMatch := matchCronField(dowField, dow, 1, 7, cronDayAliases)

	if domField == "?" {
		return dowMatch
	}
	if dowField == "?" {
		return domMatch
	}
	return domMatch && dowMatch
}

func matchCronField(field string, value, min, max int, aliases map[string]int) bool {
	f := strings.ToUpper(strings.TrimSpace(field))
	if f == "" {
		return false
	}
	if f == "*" || f == "?" {
		return true
	}

	for _, rawPart := range strings.Split(f, ",") {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			continue
		}
		if matchCronPart(part, value, min, max, aliases) {
			return true
		}
	}
	return false
}

func matchCronPart(part string, value, min, max int, aliases map[string]int) bool {
	if strings.Contains(part, "/") {
		sp := strings.SplitN(part, "/", 2)
		if len(sp) != 2 {
			return false
		}
		step, ok := parseCronAtom(sp[1], aliases)
		if !ok || step <= 0 {
			return false
		}

		start := min
		end := max
		base := strings.TrimSpace(sp[0])
		if base != "*" && base != "?" {
			if strings.Contains(base, "-") {
				r := strings.SplitN(base, "-", 2)
				if len(r) != 2 {
					return false
				}
				var okStart, okEnd bool
				start, okStart = parseCronAtom(r[0], aliases)
				end, okEnd = parseCronAtom(r[1], aliases)
				if !okStart || !okEnd {
					return false
				}
			} else {
				v, okValue := parseCronAtom(base, aliases)
				if !okValue {
					return false
				}
				start, end = v, max
			}
		}

		if value < start || value > end {
			return false
		}
		return (value-start)%step == 0
	}

	if strings.Contains(part, "-") {
		r := strings.SplitN(part, "-", 2)
		if len(r) != 2 {
			return false
		}
		start, okStart := parseCronAtom(r[0], aliases)
		end, okEnd := parseCronAtom(r[1], aliases)
		if !okStart || !okEnd {
			return false
		}
		return value >= start && value <= end
	}

	v, ok := parseCronAtom(part, aliases)
	if !ok {
		return false
	}
	return value == v
}

func parseCronAtom(v string, aliases map[string]int) (int, bool) {
	s := strings.ToUpper(strings.TrimSpace(v))
	if s == "" {
		return 0, false
	}
	if aliases != nil {
		if resolved, ok := aliases[s]; ok {
			return resolved, true
		}
	}
	parsed, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func parseSchedulerAtExpression(expr, timezone string) (time.Time, bool) {
	raw := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "at("), ")")
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
	tz := strings.TrimSpace(timezone)
	if tz == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC
	}
	return loc
}
