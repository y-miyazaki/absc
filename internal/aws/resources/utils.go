package resources

import (
	"strconv"
	"strings"
	"time"
)

func buildSlots(expr string) []int {
	slots := make([]int, slotsPerDay)
	e := strings.TrimSpace(expr)
	if strings.HasPrefix(e, "cron(") && strings.HasSuffix(e, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(e, "cron("), ")")
		fields := strings.Fields(inside)
		if len(fields) >= 2 {
			mins := parseCronField(fields[0], 0, 59)
			hours := parseCronField(fields[1], 0, 23)
			for _, h := range hours {
				for _, m := range mins {
					idx := h*6 + (m / 10)
					if idx >= 0 && idx < slotsPerDay {
						slots[idx] = 1
					}
				}
			}
		}
		return slots
	}
	if strings.HasPrefix(e, "rate(") && strings.HasSuffix(e, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(e, "rate("), ")")
		fields := strings.Fields(inside)
		if len(fields) >= 2 {
			n, err := strconv.Atoi(fields[0])
			if err == nil && n > 0 {
				unit := strings.ToLower(fields[1])
				switch {
				case strings.HasPrefix(unit, "minute"):
					for m := 0; m < 24*60; m += n {
						slots[m/10] = 1
					}
				case strings.HasPrefix(unit, "hour"):
					for h := 0; h < 24; h += n {
						slots[h*6] = 1
					}
				default:
					slots[0] = 1
				}
			}
		}
	}
	return slots
}

func parseCronField(field string, min, max int) []int {
	f := strings.TrimSpace(field)
	if f == "*" || f == "?" {
		vals := make([]int, 0, max-min+1)
		for i := min; i <= max; i++ {
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
			for i := min; i <= max; i += n {
				result[i] = struct{}{}
			}
			continue
		}
		if strings.Contains(p, "-") {
			sp := strings.SplitN(p, "-", 2)
			start, e1 := strconv.Atoi(sp[0])
			end, e2 := strconv.Atoi(sp[1])
			if e1 != nil || e2 != nil {
				continue
			}
			if start < min {
				start = min
			}
			if end > max {
				end = max
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
		if v >= min && v <= max {
			result[v] = struct{}{}
		}
	}
	vals := make([]int, 0, len(result))
	for v := range result {
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		for i := min; i <= max; i++ {
			vals = append(vals, i)
		}
	}
	return vals
}

func detectTargetKind(arn string, hasBatchParameters bool) string {
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
	if hasBatchParameters || strings.Contains(s, ":batch:") {
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
	s := strings.ToLower(arn)
	switch {
	case strings.Contains(s, ":aws-sdk:sfn:startexecution"):
		return "Step Functions"
	case strings.Contains(s, ":aws-sdk:batch:submitjob"):
		return "Batch"
	case strings.Contains(s, ":aws-sdk:ec2:"):
		return "EC2"
	case strings.Contains(s, ":aws-sdk:rds:"):
		return "RDS"
	case strings.Contains(s, ":aws-sdk:ecs:"):
		return "ECS"
	case strings.Contains(s, ":aws-sdk:glue:"):
		return "Glue"
	case strings.Contains(s, ":aws-sdk:lambda:"):
		return "Lambda"
	case strings.Contains(s, ":aws-sdk:redshift:"):
		return "Redshift"
	case strings.Contains(s, ":states:") && strings.Contains(s, ":statemachine:"):
		return "Step Functions"
	case strings.Contains(s, ":lambda:"):
		return "Lambda"
	case strings.Contains(s, ":ecs:"):
		return "ECS"
	case strings.Contains(s, ":batch:"):
		return "Batch"
	case strings.Contains(s, ":rds:"):
		return "RDS"
	case strings.Contains(s, ":events:"):
		return "EventBridge"
	default:
		return "Other"
	}
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
