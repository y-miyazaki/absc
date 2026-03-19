//revive:disable:comments-density reason: cron parsing helpers are dense but mechanically structured.
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
	"SUN": cronSunday,
	"MON": cronMonday,
	"TUE": cronTuesday,
	"WED": cronWednesday,
	"THU": cronThursday,
	"FRI": cronFriday,
	"SAT": cronSaturday,
}

var cronMonthAliases = map[string]int{
	"JAN": january,
	"FEB": february,
	"MAR": march,
	"APR": april,
	"MAY": may,
	"JUN": june,
	"JUL": july,
	"AUG": august,
	"SEP": september,
	"OCT": october,
	"NOV": november,
	"DEC": december,
}

const (
	cronCloseSuffix        = ")"
	cronDashSeparator      = "-"
	cronNoSpecificValue    = "?"
	april                  = 4
	august                 = 8
	cronDayOfMonthMax      = 31
	cronDayOfMonthMin      = 1
	cronFieldCount         = 6
	cronFriday             = 6
	cronHourMax            = 23
	cronMonday             = 2
	cronMonthMax           = 12
	cronMonthMin           = 1
	cronMinuteMax          = 59
	cronRangeSplitParts    = 2
	cronSaturday           = 7
	cronSearchYears        = 5
	cronSunday             = 1
	cronThursday           = 5
	cronTuesday            = 3
	cronWednesday          = 4
	cronYearMax            = 2199
	cronYearMin            = 1970
	dayHours               = 24
	december               = 12
	february               = 2
	january                = 1
	july                   = 7
	june                   = 6
	march                  = 3
	may                    = 5
	minuteHoursMultiplier  = 60
	november               = 11
	october                = 10
	rateExpressionMinParts = 2
	september              = 9
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
	if len(fields) != cronFieldCount {
		return false
	}

	minute := t.Minute()
	hour := t.Hour()
	dom := t.Day()
	month := int(t.Month())
	dow := int(t.Weekday())
	if dow == 0 {
		dow = cronSunday
	} else {
		dow++
	}
	year := t.Year()

	if !matchCronField(fields[0], minute, 0, cronMinuteMax, nil) {
		return false
	}
	if !matchCronField(fields[1], hour, 0, cronHourMax, nil) {
		return false
	}
	if !matchCronField(fields[3], month, cronMonthMin, cronMonthMax, cronMonthAliases) {
		return false
	}
	if !matchCronField(fields[5], year, cronYearMin, cronYearMax, nil) {
		return false
	}

	domField := strings.TrimSpace(fields[2])
	dowField := strings.TrimSpace(fields[4])
	domMatch := matchCronField(domField, dom, cronDayOfMonthMin, cronDayOfMonthMax, nil)
	dowMatch := matchCronField(dowField, dow, cronSunday, cronSaturday, cronDayAliases)

	if domField == cronNoSpecificValue {
		return dowMatch
	}
	if dowField == cronNoSpecificValue {
		return domMatch
	}
	return domMatch && dowMatch
}

func matchCronField(field string, value, minValue, maxValue int, aliases map[string]int) bool {
	f := strings.ToUpper(strings.TrimSpace(field))
	if f == "" {
		return false
	}
	if f == "*" || f == cronNoSpecificValue {
		return true
	}

	for _, rawPart := range strings.Split(f, ",") {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			continue
		}
		if matchCronPart(part, value, minValue, maxValue, aliases) {
			return true
		}
	}
	return false
}

// matchCronPart evaluates if a single cron expression part matches a value.
// Note: Similar logic exists in exporter/cron.go as matchAWSFieldPart().
// These functions should be consolidated in a future refactoring to reduce duplication.
// Currently kept separate to avoid circular package dependencies (exporter depends on resources).
func matchCronPart(part string, value, minValue, maxValue int, aliases map[string]int) bool {
	if strings.Contains(part, "/") {
		sp := strings.SplitN(part, "/", cronRangeSplitParts)
		if len(sp) != cronRangeSplitParts {
			return false
		}
		step, ok := parseCronAtom(sp[1], aliases)
		if !ok || step <= 0 {
			return false
		}

		start := minValue
		end := maxValue
		base := strings.TrimSpace(sp[0])
		if base != "*" && base != cronNoSpecificValue {
			if strings.Contains(base, cronDashSeparator) {
				r := strings.SplitN(base, cronDashSeparator, cronRangeSplitParts)
				if len(r) != cronRangeSplitParts {
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
				start, end = v, maxValue
			}
		}

		if start <= end {
			if value < start || value > end {
				return false
			}
			return (value-start)%step == 0
		}

		if value >= start {
			return (value-start)%step == 0
		}
		if value <= end {
			return ((maxValue-start+1)+(value-minValue))%step == 0
		}
		return false
	}

	if strings.Contains(part, cronDashSeparator) {
		r := strings.SplitN(part, cronDashSeparator, cronRangeSplitParts)
		if len(r) != cronRangeSplitParts {
			return false
		}
		start, okStart := parseCronAtom(r[0], aliases)
		end, okEnd := parseCronAtom(r[1], aliases)
		if !okStart || !okEnd {
			return false
		}
		if start > end {
			return value >= start || value <= end
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
