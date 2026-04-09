// Package helpers provides reusable pure utility functions.
//
//revive:disable:comments-density reason: parser helpers are intentionally compact and mechanically structured.
package helpers

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	awsCronApril            = 4
	awsCronAugust           = 8
	awsCronDashSeparator    = "-"
	awsCronDecember         = 12
	awsCronDayOfMonthMax    = 31
	awsCronDayOfMonthMin    = 1
	awsCronFieldCount       = 6
	awsCronFebruary         = 2
	awsCronFriday           = 6
	awsCronHourMax          = 23
	awsCronJanuary          = 1
	awsCronJuly             = 7
	awsCronJune             = 6
	awsCronMarch            = 3
	awsCronMay              = 5
	awsCronMinuteMax        = 59
	awsCronMonday           = 2
	awsCronMonthMax         = 12
	awsCronMonthMin         = 1
	awsCronNoSpecific       = "?"
	awsCronOpenParen        = "("
	awsCronCloseParen       = ")"
	awsCronSlashSeparator   = "/"
	awsCronWildcard         = "*"
	awsCronNovember         = 11
	awsCronOctober          = 10
	awsCronSaturday         = 7
	awsCronSeptember        = 9
	awsCronSplitParts       = 2
	awsCronSunday           = 1
	awsCronThursday         = 5
	awsCronTuesday          = 3
	awsCronWednesday        = 4
	awsCronYearMax          = 2199
	awsCronYearMin          = 1970
	dayHours                = 24
	dayMinutes              = dayHours * 60
	minutesPerHour          = 60
	rateExpressionMinFields = 2
	rateExpressionMinParts  = 2
)

var awsCronDayAliases = map[string]int{
	"SUN": awsCronSunday,
	"MON": awsCronMonday,
	"TUE": awsCronTuesday,
	"WED": awsCronWednesday,
	"THU": awsCronThursday,
	"FRI": awsCronFriday,
	"SAT": awsCronSaturday,
}

var awsCronMonthAliases = map[string]int{
	"JAN": awsCronJanuary,
	"FEB": awsCronFebruary,
	"MAR": awsCronMarch,
	"APR": awsCronApril,
	"MAY": awsCronMay,
	"JUN": awsCronJune,
	"JUL": awsCronJuly,
	"AUG": awsCronAugust,
	"SEP": awsCronSeptember,
	"OCT": awsCronOctober,
	"NOV": awsCronNovember,
	"DEC": awsCronDecember,
}

// BuildDailySlots expands an AWS cron or rate expression into one day's slot bitmap.
func BuildDailySlots(expr string, slotMinutes int) []int {
	slotsPerDay := dayMinutes / slotMinutes
	slotsPerHour := minutesPerHour / slotMinutes
	slots := make([]int, slotsPerDay)
	trimmed := strings.TrimSpace(expr)
	if strings.HasPrefix(trimmed, "cron"+awsCronOpenParen) && strings.HasSuffix(trimmed, awsCronCloseParen) {
		inside := strings.TrimSuffix(strings.TrimPrefix(trimmed, "cron"+awsCronOpenParen), awsCronCloseParen)
		fields := strings.Fields(inside)
		if len(fields) >= rateExpressionMinFields {
			minutes := ParseCronField(fields[0], 0, awsCronMinuteMax)
			hours := ParseCronField(fields[1], 0, awsCronHourMax)
			for _, hour := range hours {
				for _, minute := range minutes {
					idx := hour*slotsPerHour + (minute / slotMinutes)
					if idx >= 0 && idx < slotsPerDay {
						slots[idx] = 1
					}
				}
			}
		}
		return slots
	}
	if strings.HasPrefix(trimmed, "rate"+awsCronOpenParen) && strings.HasSuffix(trimmed, awsCronCloseParen) {
		inside := strings.TrimSuffix(strings.TrimPrefix(trimmed, "rate"+awsCronOpenParen), awsCronCloseParen)
		fields := strings.Fields(inside)
		if len(fields) >= rateExpressionMinParts {
			value, err := strconv.Atoi(fields[0])
			if err == nil && value > 0 {
				unit := strings.ToLower(fields[1])
				switch {
				case strings.HasPrefix(unit, "minute"):
					for minute := 0; minute < dayMinutes; minute += value {
						slots[minute/slotMinutes] = 1
					}
				case strings.HasPrefix(unit, "hour"):
					for hour := 0; hour < dayHours; hour += value {
						slots[hour*slotsPerHour] = 1
					}
				default:
					slots[0] = 1
				}
			}
		}
	}
	return slots
}

// MatchAWSCronExpression reports whether a timestamp matches the provided AWS cron fields.
func MatchAWSCronExpression(fields []string, candidate time.Time) bool {
	if len(fields) != awsCronFieldCount {
		return false
	}

	minute := candidate.Minute()
	hour := candidate.Hour()
	dayOfMonth := candidate.Day()
	month := int(candidate.Month())
	dayOfWeek := int(candidate.Weekday())
	if dayOfWeek == 0 {
		dayOfWeek = awsCronSunday
	} else {
		dayOfWeek++
	}
	year := candidate.Year()

	if !MatchCronField(fields[0], minute, 0, awsCronMinuteMax, nil) {
		return false
	}
	if !MatchCronField(fields[1], hour, 0, awsCronHourMax, nil) {
		return false
	}
	if !MatchCronField(fields[3], month, awsCronMonthMin, awsCronMonthMax, awsCronMonthAliases) {
		return false
	}
	if !MatchCronField(fields[5], year, awsCronYearMin, awsCronYearMax, nil) {
		return false
	}

	domField := strings.TrimSpace(fields[2])
	dowField := strings.TrimSpace(fields[4])
	domMatch := MatchCronField(domField, dayOfMonth, awsCronDayOfMonthMin, awsCronDayOfMonthMax, nil)
	dowMatch := MatchCronField(dowField, dayOfWeek, awsCronSunday, awsCronSaturday, awsCronDayAliases)
	if domField == awsCronNoSpecific {
		return dowMatch
	}
	if dowField == awsCronNoSpecific {
		return domMatch
	}
	return domMatch && dowMatch
}

// MatchCronField reports whether a single cron field matches the provided value.
func MatchCronField(field string, value, minValue, maxValue int, aliases map[string]int) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(field))
	if trimmed == "" {
		return false
	}
	if trimmed == awsCronWildcard || trimmed == awsCronNoSpecific {
		return true
	}
	for _, rawPart := range strings.Split(trimmed, ",") {
		part := strings.TrimSpace(rawPart)
		if part != "" && MatchCronPart(part, value, minValue, maxValue, aliases) {
			return true
		}
	}
	return false
}

// MatchCronPart reports whether one cron token or token range matches the provided value.
func MatchCronPart(part string, value, minValue, maxValue int, aliases map[string]int) bool {
	if strings.Contains(part, awsCronSlashSeparator) {
		pieces := strings.SplitN(part, awsCronSlashSeparator, awsCronSplitParts)
		if len(pieces) != awsCronSplitParts {
			return false
		}
		step, ok := ParseCronAtom(pieces[1], aliases)
		if !ok || step <= 0 {
			return false
		}

		start := minValue
		end := maxValue
		base := strings.TrimSpace(pieces[0])
		if base != awsCronWildcard && base != awsCronNoSpecific {
			if strings.Contains(base, awsCronDashSeparator) {
				rangeParts := strings.SplitN(base, awsCronDashSeparator, awsCronSplitParts)
				if len(rangeParts) != awsCronSplitParts {
					return false
				}
				var okStart, okEnd bool
				start, okStart = ParseCronAtom(rangeParts[0], aliases)
				end, okEnd = ParseCronAtom(rangeParts[1], aliases)
				if !okStart || !okEnd {
					return false
				}
			} else {
				var okStart bool
				start, okStart = ParseCronAtom(base, aliases)
				if !okStart {
					return false
				}
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

	if strings.Contains(part, awsCronDashSeparator) {
		rangeParts := strings.SplitN(part, awsCronDashSeparator, awsCronSplitParts)
		if len(rangeParts) != awsCronSplitParts {
			return false
		}
		start, okStart := ParseCronAtom(rangeParts[0], aliases)
		end, okEnd := ParseCronAtom(rangeParts[1], aliases)
		if !okStart || !okEnd {
			return false
		}
		if start > end {
			return value >= start || value <= end
		}
		return value >= start && value <= end
	}

	v, ok := ParseCronAtom(part, aliases)
	return ok && value == v
}

// ParseCronAtom parses a single cron atom using optional aliases.
func ParseCronAtom(value string, aliases map[string]int) (int, bool) {
	trimmed := strings.ToUpper(strings.TrimSpace(value))
	if trimmed == "" {
		return 0, false
	}
	if aliases != nil {
		if resolved, ok := aliases[trimmed]; ok {
			return resolved, true
		}
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// ParseCronField expands a cron field into the discrete values it can match.
func ParseCronField(field string, minValue, maxValue int) []int {
	trimmed := strings.TrimSpace(field)
	if trimmed == awsCronWildcard || trimmed == awsCronNoSpecific {
		values := make([]int, 0, maxValue-minValue+1)
		for i := minValue; i <= maxValue; i++ {
			values = append(values, i)
		}
		return values
	}

	result := make(map[int]struct{})
	for _, rawPart := range strings.Split(trimmed, ",") {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			continue
		}
		if strings.Contains(part, awsCronSlashSeparator) {
			stepParts := strings.SplitN(part, awsCronSlashSeparator, awsCronSplitParts)
			if len(stepParts) != awsCronSplitParts {
				continue
			}
			step, err := strconv.Atoi(strings.TrimSpace(stepParts[1]))
			if err != nil || step <= 0 {
				continue
			}

			start := minValue
			end := maxValue
			base := strings.TrimSpace(stepParts[0])
			if base != awsCronWildcard && base != awsCronNoSpecific {
				if strings.Contains(base, awsCronDashSeparator) {
					rangeParts := strings.SplitN(base, awsCronDashSeparator, awsCronSplitParts)
					if len(rangeParts) != awsCronSplitParts {
						continue
					}
					var parseErr error
					start, parseErr = strconv.Atoi(strings.TrimSpace(rangeParts[0]))
					if parseErr != nil {
						continue
					}
					end, parseErr = strconv.Atoi(strings.TrimSpace(rangeParts[1]))
					if parseErr != nil {
						continue
					}
				} else {
					var parseErr error
					start, parseErr = strconv.Atoi(base)
					if parseErr != nil {
						continue
					}
				}
			}

			if start < minValue {
				start = minValue
			}
			if end > maxValue {
				end = maxValue
			}
			if start > maxValue || end < minValue {
				continue
			}
			if start <= end {
				for i := start; i <= end; i += step {
					result[i] = struct{}{}
				}
				continue
			}

			cycleLength := (maxValue - start + 1) + (end - minValue + 1)
			for offset := 0; offset < cycleLength; offset += step {
				candidate := start + offset
				if candidate > maxValue {
					candidate = minValue + (candidate - maxValue - 1)
				}
				if candidate >= minValue && candidate <= maxValue {
					result[candidate] = struct{}{}
				}
			}
			continue
		}

		if strings.Contains(part, awsCronDashSeparator) {
			rangeParts := strings.SplitN(part, awsCronDashSeparator, awsCronSplitParts)
			if len(rangeParts) != awsCronSplitParts {
				continue
			}
			start, startErr := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			end, endErr := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if startErr != nil || endErr != nil {
				continue
			}
			if start < minValue {
				start = minValue
			}
			if end > maxValue {
				end = maxValue
			}
			if start > end {
				for i := start; i <= maxValue; i++ {
					result[i] = struct{}{}
				}
				for i := minValue; i <= end; i++ {
					result[i] = struct{}{}
				}
				continue
			}
			for i := start; i <= end; i++ {
				result[i] = struct{}{}
			}
			continue
		}

		parsed, err := strconv.Atoi(part)
		if err == nil && parsed >= minValue && parsed <= maxValue {
			result[parsed] = struct{}{}
		}
	}

	values := make([]int, 0, len(result))
	for value := range result {
		values = append(values, value)
	}
	slices.Sort(values)
	if len(values) == 0 {
		for i := minValue; i <= maxValue; i++ {
			values = append(values, i)
		}
	}
	return values
}
