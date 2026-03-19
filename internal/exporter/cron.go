//revive:disable:comments-density reason: data-shaping code is clearer without line-by-line commentary.
package exporter

import (
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/y-miyazaki/absc/internal/aws/resources"
)

const (
	awsCronApril                       = 4
	awsCronAugust                      = 8
	awsCronDayOfMonthMax               = 31
	awsCronDayOfMonthMin               = 1
	awsCronDecember                    = 12
	awsCronDashSeparator               = "-"
	awsCronFebruary                    = 2
	awsCronFieldCount                  = 6
	awsCronFriday                      = 6
	awsCronHourMax                     = 23
	awsCronJanuary                     = 1
	awsCronJuly                        = 7
	awsCronJune                        = 6
	awsCronMarch                       = 3
	awsCronMay                         = 5
	awsCronMinuteMax                   = 59
	awsCronMonday                      = 2
	awsCronMonthMax                    = 12
	awsCronMonthMin                    = 1
	awsCronNoSpecific                  = "?"
	awsCronNovember                    = 11
	awsCronOctober                     = 10
	awsCronSaturday                    = 7
	awsCronSeptember                   = 9
	awsCronSplitParts                  = 2
	awsCronSunday                      = 1
	awsCronThursday                    = 5
	awsCronTuesday                     = 3
	awsCronWednesday                   = 4
	awsCronYearMax                     = 2199
	awsCronYearMin                     = 1970
	defaultDirPermission               = 0o750
	defaultFilePermission              = 0o600
	hourLabelStep                      = 1
	hoursPerDay                        = 24
	minutesPerSlot                     = 10
	outputVersion                      = "1.0"
	runInSlotCategoryNotObservable     = "not_observable_target"
	runInSlotCategoryNotScheduledToday = "not_scheduled_today"
	runInSlotCategoryObservable        = "observable"
	secondsPerHour                     = 3600
	secondsPerMinute                   = 60
	slotIssueCollectedRunsCapped       = "COLLECTED_RUNS_CAPPED"
	slotIssueNoRunInWindow             = "NO_RUN_IN_WINDOW"
	slotIssueRunNotAlignedToSlot       = "RUN_NOT_ALIGNED_TO_SLOT"
	slotMinutes                        = 10
	slotsPerHour                       = 6
	slotsPerTimelineDay                = 144
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

//go:embed html_template.html
var htmlTemplate string

//go:embed assets/icons/*.svg
var iconAssets embed.FS

//go:embed slot_issue_policy.json
var slotIssuePolicyJSON string

//nolint:tagliatelle // Output is a stable external snake_case JSON schema.
type Output struct {
	Version     string           `json:"version"`
	GeneratedAt string           `json:"generated_at"`
	AccountID   string           `json:"account_id"`
	Timezone    string           `json:"timezone"`
	Schedules   []Schedule       `json:"schedules"`
	Alignment   []AlignmentIssue `json:"alignment_issues,omitempty"`
	Errors      []ErrRecord      `json:"errors"`
	Window      Window           `json:"window"`
}

type BuildOutputOptions struct {
	IncludeNonSlotRuns bool
}

//nolint:tagliatelle // slotIssuePolicy reads a stable snake_case JSON file.
type slotIssuePolicy struct {
	ObservableTargetKinds []string `json:"observable_target_kinds"`
}

//nolint:tagliatelle // SlotRunIssue is a stable external snake_case JSON schema.
type SlotRunIssue struct {
	SlotLabel string `json:"slot_label"`
	Reason    string `json:"reason"`
	SlotIndex int    `json:"slot_index"`
}

//nolint:tagliatelle // AlignmentIssue is a stable external snake_case JSON schema.
type AlignmentIssue struct {
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	RunID        string `json:"run_id"`
	RunStartAt   string `json:"run_start_at"`
	RunEndAt     string `json:"run_end_at,omitempty"`
	Reason       string `json:"reason"`
}

//nolint:tagliatelle // Window is a stable external snake_case JSON schema.
type Window struct {
	Start       string   `json:"start"`
	End         string   `json:"end"`
	HourLabels  []string `json:"hour_labels,omitempty"`
	SlotLabels  []string `json:"slot_labels,omitempty"`
	SlotMinutes int      `json:"slot_minutes"`
}

//nolint:tagliatelle // Schedule is a stable external snake_case JSON schema.
type Schedule struct {
	TargetAction               string         `json:"target_action,omitempty"`
	ID                         string         `json:"id"`
	ScheduleName               string         `json:"schedule_name"`
	ScheduleExpression         string         `json:"schedule_expression"`
	ScheduleExpressionTimezone string         `json:"schedule_expression_timezone,omitempty"`
	ScheduleExpressionTZLabel  string         `json:"schedule_expression_timezone_label,omitempty"`
	NextInvocationAt           string         `json:"next_invocation_at,omitempty"`
	NextInvocationLabel        string         `json:"next_invocation_label,omitempty"`
	Service                    string         `json:"service"`
	TargetKind                 string         `json:"target_kind"`
	TargetName                 string         `json:"target_name,omitempty"`
	TargetService              string         `json:"target_service"`
	Region                     string         `json:"region"`
	TargetARN                  string         `json:"target_arn"`
	RunInSlotCategory          string         `json:"run_in_slot_category"`
	Runs                       []Run          `json:"runs"`
	SlotRunIssues              []SlotRunIssue `json:"slot_run_issues,omitempty"`
	Slots                      []int          `json:"slots"`
	ExpectedInWindow           bool           `json:"expected_in_window"`
	Enabled                    bool           `json:"enabled"`
	RunsCapped                 bool           `json:"runs_capped,omitempty"`
}

//nolint:tagliatelle // Run is a stable external snake_case JSON schema.
type Run struct {
	RunID         string `json:"run_id"`
	Status        string `json:"status"`
	StartAt       string `json:"start_at,omitempty"`
	StartLabel    string `json:"start_label,omitempty"`
	EndAt         string `json:"end_at,omitempty"`
	EndLabel      string `json:"end_label,omitempty"`
	DurationSec   *int64 `json:"duration_sec,omitempty"`
	SourceService string `json:"source_service"`
}

//nolint:tagliatelle // ErrRecord is a stable external snake_case JSON schema.
type ErrRecord struct {
	Service string `json:"service"`
	Region  string `json:"region"`
	Message string `json:"message"`
}

func WriteJSON(path string, out *Output) (retErr error) {
	cleanPath := filepath.Clean(path)
	f, err := os.Create(cleanPath) // #nosec G304 -- validated application output path
	if err != nil {
		return fmt.Errorf("create json file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close json file: %w", closeErr)
		}
	}()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if encodeErr := enc.Encode(out); encodeErr != nil {
		return fmt.Errorf("encode json: %w", encodeErr)
	}
	return nil
}

func WriteSlotRunIssuesCSV(path string, out *Output) (retErr error) {
	cleanPath := filepath.Clean(path)
	f, err := os.Create(cleanPath) // #nosec G304 -- validated application output path
	if err != nil {
		return fmt.Errorf("create slot run issues csv file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close slot run issues csv file: %w", closeErr)
		}
	}()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"schedule_id", "schedule_name", "region", "target_service", "slot_index", "slot_label", "reason", "runs_capped"}
	if writeErr := w.Write(header); writeErr != nil {
		return fmt.Errorf("write csv header: %w", writeErr)
	}

	for i := range out.Schedules {
		s := out.Schedules[i]
		for j := range s.SlotRunIssues {
			issue := s.SlotRunIssues[j]
			row := []string{
				s.ID,
				s.ScheduleName,
				s.Region,
				s.TargetService,
				strconv.Itoa(issue.SlotIndex),
				issue.SlotLabel,
				issue.Reason,
				strconv.FormatBool(s.RunsCapped),
			}
			if writeErr := w.Write(row); writeErr != nil {
				return fmt.Errorf("write csv row: %w", writeErr)
			}
		}
	}

	if flushErr := w.Error(); flushErr != nil {
		return fmt.Errorf("flush csv writer: %w", flushErr)
	}

	return nil
}

const errorsHTMLTemplate = `<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8" />
	<meta name="viewport" content="width=device-width, initial-scale=1" />
	<title>ABSC Errors</title>
	<style>
		body { font-family: "Noto Sans", "Segoe UI", sans-serif; margin: 24px; color: #1d2730; }
		h1 { margin: 0 0 8px; }
		.meta { color: #5f6b76; margin-bottom: 14px; font-size: 13px; }
		.toplink { margin-bottom: 12px; }
		table { border-collapse: collapse; width: 100%; }
		th, td { border: 1px solid #d7e0e7; padding: 8px; text-align: left; vertical-align: top; }
		th { background: #f3f8f8; }
		.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; white-space: pre-wrap; }
	</style>
</head>
<body>
	<h1>ABSC Errors</h1>
	<div class="meta">Generated at {{ .GeneratedAt }} | Account: {{ .AccountID }} | Timezone: {{ .Timezone }} | Count: {{ len .Errors }}</div>
	<div class="toplink"><a href="index.html">&larr; Back to timeline</a></div>
	{{ if .Errors }}
	<table>
		<tr><th>Service</th><th>Region</th><th>Message</th></tr>
		{{ range .Errors }}
		<tr>
			<td>{{ .Service }}</td>
			<td>{{ .Region }}</td>
			<td class="mono">{{ .Message }}</td>
		</tr>
		{{ end }}
	</table>
	{{ else }}
	<p>No errors.</p>
	{{ end }}
</body>
</html>`

func WriteErrorsHTML(path string, out *Output) (retErr error) {
	tpl, err := template.New("errors").Parse(errorsHTMLTemplate)
	if err != nil {
		return fmt.Errorf("parse errors html template: %w", err)
	}

	cleanPath := filepath.Clean(path)
	f, err := os.Create(cleanPath) // #nosec G304 -- validated application output path
	if err != nil {
		return fmt.Errorf("create errors html file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close errors html file: %w", closeErr)
		}
	}()

	if execErr := tpl.Execute(f, out); execErr != nil {
		return fmt.Errorf("render errors html file: %w", execErr)
	}

	return nil
}

func BuildOutput(accountID string, now, since time.Time, loc *time.Location, schedules []resources.Schedule, errs []resources.ErrorRecord) Output {
	return BuildOutputWithOptions(accountID, now, since, loc, schedules, errs, BuildOutputOptions{})
}

func BuildOutputWithOptions(accountID string, now, since time.Time, loc *time.Location, schedules []resources.Schedule, errs []resources.ErrorRecord, options BuildOutputOptions) Output {
	// Anchor the window to the local calendar day of `since` (lookback start).
	// With the default 24h lookback, this shows the previous day's full timeline.
	sinceInLoc := since.In(loc)
	dayStart := time.Date(sinceInLoc.Year(), sinceInLoc.Month(), sinceInLoc.Day(), 0, 0, 0, 0, loc)
	windowEnd := dayStart.Add(hoursPerDay * time.Hour)
	out := Output{
		Version:     outputVersion,
		GeneratedAt: now.Format(time.RFC3339),
		AccountID:   accountID,
		Timezone:    loc.String(),
		Window: Window{
			Start:       dayStart.Format(time.RFC3339),
			End:         dayStart.Add(hoursPerDay * time.Hour).Format(time.RFC3339),
			SlotMinutes: slotMinutes,
			HourLabels:  buildHourLabels(dayStart),
			SlotLabels:  buildSlotLabels(dayStart),
		},
		Schedules: make([]Schedule, 0, len(schedules)),
		Alignment: make([]AlignmentIssue, 0),
		Errors:    make([]ErrRecord, 0, len(errs)),
	}

	observableTargetKinds := loadObservableTargetKinds()

	for scheduleIndex := range schedules {
		s := schedules[scheduleIndex]
		sourceLoc := scheduleSourceLocation(s.ScheduleExpressionTimezone)
		slots := remapSlotsToLocation(s.Slots, dayStart, sourceLoc, loc)
		windowRuns := make([]Run, 0, len(s.Runs))
		for runIndex := range s.Runs {
			r := s.Runs[runIndex]
			startAt := convertRFC3339ToLocation(r.StartAt, loc)
			endAt := convertRFC3339ToLocation(r.EndAt, loc)
			if !runOverlapsWindow(startAt, endAt, dayStart, windowEnd) {
				continue
			}
			windowRuns = append(windowRuns, Run{
				RunID:         r.RunID,
				Status:        r.Status,
				StartAt:       startAt,
				StartLabel:    formatDisplayTimestamp(startAt, loc),
				EndAt:         endAt,
				EndLabel:      formatDisplayTimestamp(endAt, loc),
				DurationSec:   r.DurationSec,
				SourceService: r.SourceService,
			})
		}

		alignedRuns := make([]Run, 0, len(windowRuns))
		misalignedRuns := make([]Run, 0)
		for runIndex := range windowRuns {
			run := windowRuns[runIndex]
			if runOverlapsScheduledSlots(&run, slots, dayStart) {
				alignedRuns = append(alignedRuns, run)
				continue
			}
			misalignedRuns = append(misalignedRuns, run)
		}

		runs := alignedRuns
		if options.IncludeNonSlotRuns {
			runs = windowRuns
		}

		isObservableTarget := isObservableTargetKind(s.TargetKind, observableTargetKinds)
		expectedInWindow := scheduleExpectedInWindow(s.ScheduleExpression, s.ScheduleExpressionTimezone, dayStart, windowEnd)
		runInSlotCategory := runInSlotCategoryObservable
		if !isObservableTarget {
			runInSlotCategory = runInSlotCategoryNotObservable
		} else if !expectedInWindow {
			runInSlotCategory = runInSlotCategoryNotScheduledToday
		}
		emitSlotIssues := runInSlotCategory == runInSlotCategoryObservable
		var slotIssues []SlotRunIssue
		if s.Enabled && emitSlotIssues {
			issueReason := slotIssueNoRunInWindow
			if s.RunsCapped {
				issueReason = slotIssueCollectedRunsCapped
			} else if len(windowRuns) > 0 {
				issueReason = slotIssueRunNotAlignedToSlot
			}
			slotIssues = buildSlotRunIssues(slots, windowRuns, dayStart, issueReason)
		}
		nextInvocationAt := convertRFC3339ToLocation(s.NextInvocationAt, loc)
		out.Schedules = append(out.Schedules, Schedule{
			ID:                         s.ID,
			Service:                    s.Service,
			ScheduleName:               s.ScheduleName,
			ScheduleExpression:         s.ScheduleExpression,
			ScheduleExpressionTimezone: s.ScheduleExpressionTimezone,
			ScheduleExpressionTZLabel:  scheduleTimezoneLabel(s.ScheduleExpressionTimezone, dayStart),
			Enabled:                    s.Enabled,
			Region:                     s.Region,
			TargetARN:                  s.TargetARN,
			TargetKind:                 s.TargetKind,
			TargetAction:               s.TargetAction,
			TargetService:              s.TargetService,
			TargetName:                 s.TargetName,
			NextInvocationAt:           nextInvocationAt,
			NextInvocationLabel:        formatDisplayTimestamp(nextInvocationAt, loc),
			Slots:                      slots,
			SlotRunIssues:              slotIssues,
			ExpectedInWindow:           expectedInWindow,
			RunInSlotCategory:          runInSlotCategory,
			RunsCapped:                 s.RunsCapped,
			Runs:                       runs,
		})

		for runIndex := range misalignedRuns {
			run := misalignedRuns[runIndex]
			out.Alignment = append(out.Alignment, AlignmentIssue{
				ScheduleID:   s.ID,
				ScheduleName: s.ScheduleName,
				RunID:        run.RunID,
				RunStartAt:   run.StartAt,
				RunEndAt:     run.EndAt,
				Reason:       "run does not overlap any scheduled slot in the displayed window",
			})
		}
	}

	seenErrors := make(map[string]struct{}, len(errs))
	for errorIndex := range errs {
		e := errs[errorIndex]
		key := e.Service + "|" + e.Region + "|" + e.Message
		if _, ok := seenErrors[key]; ok {
			continue
		}
		seenErrors[key] = struct{}{}
		out.Errors = append(out.Errors, ErrRecord{Service: e.Service, Region: e.Region, Message: e.Message})
	}

	return out
}

func buildSlotRunIssues(slots []int, runs []Run, windowStart time.Time, reason string) []SlotRunIssue {
	if len(slots) != slotsPerTimelineDay {
		return nil
	}

	issues := make([]SlotRunIssue, 0)
	for idx, v := range slots {
		if v != 1 {
			continue
		}
		if slotHasRunOverlap(idx, runs, windowStart) {
			continue
		}
		issues = append(issues, SlotRunIssue{
			SlotIndex: idx,
			SlotLabel: buildSlotLabel(windowStart, idx),
			Reason:    reason,
		})
	}
	return issues
}

func isObservableTargetKind(targetKind string, observableKinds map[string]struct{}) bool {
	_, ok := observableKinds[strings.ToLower(strings.TrimSpace(targetKind))]
	return ok
}

func loadObservableTargetKinds() map[string]struct{} {
	defaults := map[string]struct{}{
		"batch":         {},
		"ecs":           {},
		"glue":          {},
		"lambda":        {},
		"stepfunctions": {},
	}

	raw := strings.TrimSpace(slotIssuePolicyJSON)
	if raw == "" {
		return defaults
	}

	var policy slotIssuePolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return defaults
	}

	set := make(map[string]struct{}, len(policy.ObservableTargetKinds))
	for i := range policy.ObservableTargetKinds {
		kind := strings.ToLower(strings.TrimSpace(policy.ObservableTargetKinds[i]))
		if kind == "" {
			continue
		}
		set[kind] = struct{}{}
	}

	if len(set) == 0 {
		return defaults
	}

	return set
}

func scheduleExpectedInWindow(expression, expressionTimezone string, windowStart, windowEnd time.Time) bool {
	expr := strings.TrimSpace(expression)
	if !strings.HasPrefix(expr, "cron(") || !strings.HasSuffix(expr, ")") {
		return true
	}

	inside := strings.TrimSuffix(strings.TrimPrefix(expr, "cron("), ")")
	fields := strings.Fields(inside)
	if len(fields) != awsCronFieldCount {
		return true
	}

	loc := scheduleSourceLocation(expressionTimezone)
	start := windowStart.In(loc).Truncate(time.Minute)
	end := windowEnd.In(loc)
	for candidate := start; candidate.Before(end); candidate = candidate.Add(time.Minute) {
		if matchAWSCronExpression(fields, candidate) {
			return true
		}
	}

	return false
}

func matchAWSCronExpression(fields []string, t time.Time) bool {
	if len(fields) != awsCronFieldCount {
		return false
	}

	minute := t.Minute()
	hour := t.Hour()
	dayOfMonth := t.Day()
	month := int(t.Month())
	dayOfWeek := int(t.Weekday())
	if dayOfWeek == 0 {
		dayOfWeek = awsCronSunday
	} else {
		dayOfWeek++
	}
	year := t.Year()

	if !matchAWSField(fields[0], minute, 0, awsCronMinuteMax, nil) {
		return false
	}
	if !matchAWSField(fields[1], hour, 0, awsCronHourMax, nil) {
		return false
	}
	if !matchAWSField(fields[3], month, awsCronMonthMin, awsCronMonthMax, awsCronMonthAliases) {
		return false
	}
	if !matchAWSField(fields[5], year, awsCronYearMin, awsCronYearMax, nil) {
		return false
	}

	domField := strings.TrimSpace(fields[2])
	dowField := strings.TrimSpace(fields[4])
	domMatch := matchAWSField(domField, dayOfMonth, awsCronDayOfMonthMin, awsCronDayOfMonthMax, nil)
	dowMatch := matchAWSField(dowField, dayOfWeek, awsCronSunday, awsCronSaturday, awsCronDayAliases)

	if domField == awsCronNoSpecific {
		return dowMatch
	}
	if dowField == awsCronNoSpecific {
		return domMatch
	}
	return domMatch && dowMatch
}

func matchAWSField(field string, value, minValue, maxValue int, aliases map[string]int) bool {
	f := strings.ToUpper(strings.TrimSpace(field))
	if f == "" {
		return false
	}
	if f == "*" || f == awsCronNoSpecific {
		return true
	}

	for _, rawPart := range strings.Split(f, ",") {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			continue
		}
		if matchAWSFieldPart(part, value, minValue, maxValue, aliases) {
			return true
		}
	}

	return false
}

// matchAWSFieldPart evaluates if a single AWS cron expression field part matches a value.
// Supports ranges, steps, and aliases with wrapping range support (e.g., "FRI-MON" or "50-10").
// Note: Similar logic exists in resources/scheduler_next_invocation.go as matchCronPart().
// These functions are kept separate to avoid circular package dependencies (exporter depends on resources).
// A future refactoring should consolidate this logic in a shared cron utilities package.
func matchAWSFieldPart(part string, value, minValue, maxValue int, aliases map[string]int) bool {
	if strings.Contains(part, "/") {
		pieces := strings.SplitN(part, "/", awsCronSplitParts)
		if len(pieces) != awsCronSplitParts {
			return false
		}
		step, ok := parseAWSAtom(pieces[1], aliases)
		if !ok || step <= 0 {
			return false
		}

		start := minValue
		end := maxValue
		base := strings.TrimSpace(pieces[0])
		if base != "*" && base != awsCronNoSpecific {
			if strings.Contains(base, awsCronDashSeparator) {
				rangeParts := strings.SplitN(base, awsCronDashSeparator, awsCronSplitParts)
				if len(rangeParts) != awsCronSplitParts {
					return false
				}
				startValue, okStart := parseAWSAtom(rangeParts[0], aliases)
				endValue, okEnd := parseAWSAtom(rangeParts[1], aliases)
				if !okStart || !okEnd {
					return false
				}
				start = startValue
				end = endValue
			} else {
				startValue, okStart := parseAWSAtom(base, aliases)
				if !okStart {
					return false
				}
				start = startValue
			}
		}

		if start <= end {
			if value < start || value > end {
				return false
			}
			return ((value - start) % step) == 0
		}

		if value >= start {
			return ((value - start) % step) == 0
		}
		if value <= end {
			return (((maxValue - start + 1) + (value - minValue)) % step) == 0
		}
		return false
	}

	if strings.Contains(part, awsCronDashSeparator) {
		rangeParts := strings.SplitN(part, awsCronDashSeparator, awsCronSplitParts)
		if len(rangeParts) != awsCronSplitParts {
			return false
		}
		startValue, okStart := parseAWSAtom(rangeParts[0], aliases)
		endValue, okEnd := parseAWSAtom(rangeParts[1], aliases)
		if !okStart || !okEnd {
			return false
		}
		if startValue > endValue {
			return value >= startValue || value <= endValue
		}
		return value >= startValue && value <= endValue
	}

	v, ok := parseAWSAtom(part, aliases)
	if !ok {
		return false
	}
	return value == v
}

func parseAWSAtom(atom string, aliases map[string]int) (int, bool) {
	a := strings.ToUpper(strings.TrimSpace(atom))
	if a == "" {
		return 0, false
	}
	if aliases != nil {
		if mapped, ok := aliases[a]; ok {
			return mapped, true
		}
	}
	v, err := strconv.Atoi(a)
	if err != nil {
		return 0, false
	}
	return v, true
}

func buildSlotLabel(windowStart time.Time, slotIndex int) string {
	start := windowStart.Add(time.Duration(slotIndex*slotMinutes) * time.Minute)
	end := start.Add(time.Duration(slotMinutes) * time.Minute)
	return start.Format("15:04") + " - " + end.Format("15:04")
}

func slotHasRunOverlap(slotIndex int, runs []Run, windowStart time.Time) bool {
	slotStart := windowStart.Add(time.Duration(slotIndex*slotMinutes) * time.Minute)
	slotEnd := slotStart.Add(time.Duration(slotMinutes) * time.Minute)
	for runIndex := range runs {
		r := runs[runIndex]
		start, err := time.Parse(time.RFC3339, strings.TrimSpace(r.StartAt))
		if err != nil {
			continue
		}
		end := start
		trimmedEnd := strings.TrimSpace(r.EndAt)
		if trimmedEnd != "" {
			if parsedEnd, parseErr := time.Parse(time.RFC3339, trimmedEnd); parseErr == nil {
				end = parsedEnd
			}
		}
		if start.Before(slotEnd) && (end.Equal(slotStart) || end.After(slotStart)) {
			return true
		}
	}
	return false
}

func buildHourLabels(dayStart time.Time) []string {
	labels := make([]string, 0, hoursPerDay/hourLabelStep)
	for hour := 0; hour < hoursPerDay; hour += hourLabelStep {
		labels = append(labels, dayStart.Add(time.Duration(hour)*time.Hour).Format("15"))
	}
	return labels
}

func buildSlotLabels(dayStart time.Time) []string {
	labels := make([]string, 0, slotsPerTimelineDay)
	for i := 0; i < slotsPerTimelineDay; i++ {
		start := dayStart.Add(time.Duration(i*slotMinutes) * time.Minute)
		end := start.Add(time.Duration(slotMinutes) * time.Minute)
		labels = append(labels, start.Format("15:04")+" - "+end.Format("15:04"))
	}
	return labels
}

func formatDisplayTimestamp(value string, loc *time.Location) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return value
	}
	return t.In(loc).Format("2006-01-02 15:04:05 MST")
}

func runOverlapsWindow(startAt, endAt string, windowStart, windowEnd time.Time) bool {
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(startAt))
	if err != nil {
		return true
	}
	end := start
	trimmedEnd := strings.TrimSpace(endAt)
	if trimmedEnd != "" {
		if parsedEnd, parseErr := time.Parse(time.RFC3339, trimmedEnd); parseErr == nil {
			end = parsedEnd
		}
	}
	return start.Before(windowEnd) && (end.Equal(windowStart) || end.After(windowStart))
}

func runOverlapsScheduledSlots(run *Run, slots []int, windowStart time.Time) bool {
	if len(slots) != slotsPerTimelineDay {
		return true
	}
	start, err := time.Parse(time.RFC3339, strings.TrimSpace(run.StartAt))
	if err != nil {
		return true
	}
	end := start
	trimmedEnd := strings.TrimSpace(run.EndAt)
	if trimmedEnd != "" {
		if parsedEnd, parseErr := time.Parse(time.RFC3339, trimmedEnd); parseErr == nil {
			end = parsedEnd
		}
	}

	windowEnd := windowStart.Add(hoursPerDay * time.Hour)
	if end.Before(windowStart) || !start.Before(windowEnd) {
		return true
	}

	slotDuration := time.Duration(slotMinutes) * time.Minute
	clampedStart := start
	if clampedStart.Before(windowStart) {
		clampedStart = windowStart
	}
	clampedEnd := end
	if !clampedEnd.Before(windowEnd) {
		clampedEnd = windowEnd.Add(-time.Nanosecond)
	}

	startIdx := int(clampedStart.Sub(windowStart) / slotDuration)
	endIdx := int(clampedEnd.Sub(windowStart) / slotDuration)
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx >= slotsPerTimelineDay {
		endIdx = slotsPerTimelineDay - 1
	}
	if endIdx < startIdx {
		return true
	}

	for i := startIdx; i <= endIdx; i++ {
		if slots[i] == 1 {
			return true
		}
	}
	return false
}

// convertRFC3339ToLocation converts canonical UTC/offset timestamps into
// the user-selected CLI timezone for output rendering.
func convertRFC3339ToLocation(value string, loc *time.Location) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return value
	}
	return t.In(loc).Format(time.RFC3339)
}

func remapSlotsToLocation(slots []int, now time.Time, srcLoc, dstLoc *time.Location) []int {
	if len(slots) != slotsPerTimelineDay {
		return slots
	}
	if srcLoc.String() == dstLoc.String() {
		copied := make([]int, len(slots))
		copy(copied, slots)
		return copied
	}

	result := make([]int, len(slots))
	sourceDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, srcLoc)
	for i, v := range slots {
		if v != 1 {
			continue
		}
		sourceTime := sourceDay.Add(time.Duration(i*slotMinutes) * time.Minute)
		destinationTime := sourceTime.In(dstLoc)
		idx := destinationTime.Hour()*slotsPerHour + (destinationTime.Minute() / minutesPerSlot)
		if idx >= 0 && idx < len(result) {
			result[idx] = 1
		}
	}
	return result
}

func scheduleSourceLocation(exprTZ string) *time.Location {
	tz := strings.TrimSpace(exprTZ)
	if tz == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC
	}
	return loc
}

func scheduleTimezoneLabel(exprTZ string, reference time.Time) string {
	raw := strings.TrimSpace(exprTZ)
	loc := scheduleSourceLocation(raw)
	name := raw
	if name == "" {
		name = loc.String()
	}
	if name == "" {
		name = "UTC"
	}
	_, offsetSeconds := reference.In(loc).Zone()
	return name + " (" + formatUTCOffset(offsetSeconds) + ")"
}

func formatUTCOffset(offsetSeconds int) string {
	sign := "+"
	absOffsetSeconds := offsetSeconds
	if absOffsetSeconds < 0 {
		sign = "-"
		absOffsetSeconds = -absOffsetSeconds
	}
	hours := absOffsetSeconds / secondsPerHour
	minutes := (absOffsetSeconds % secondsPerHour) / secondsPerMinute
	return fmt.Sprintf("UTC%s%02d:%02d", sign, hours, minutes)
}

func WriteHTML(path string, out *Output) (retErr error) {
	// Output contains only string/struct/slice fields; json.Marshal is guaranteed to succeed.
	b, _ := json.Marshal(out)

	html := htmlTemplate
	html = strings.ReplaceAll(html, "@@INDEX_TITLE@@", "ABSC Cron Timeline")
	html = strings.ReplaceAll(html, "@@PAYLOAD@@", string(b))

	cleanPath := filepath.Clean(path)
	f, err := os.Create(cleanPath) // #nosec G304 -- validated application output path
	if err != nil {
		return fmt.Errorf("create html file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close html file: %w", closeErr)
		}
	}()

	if _, writeErr := f.WriteString(html); writeErr != nil {
		return fmt.Errorf("write html file: %w", writeErr)
	}

	if iconErr := writeIconAssets(filepath.Dir(cleanPath)); iconErr != nil {
		return fmt.Errorf("write html assets: %w", iconErr)
	}

	return nil
}

func writeIconAssets(baseDir string) error {
	iconsDir := filepath.Join(baseDir, "assets", "icons")
	if err := os.MkdirAll(iconsDir, defaultDirPermission); err != nil {
		return fmt.Errorf("create icons directory: %w", err)
	}

	entries, err := iconAssets.ReadDir("assets/icons")
	if err != nil {
		return fmt.Errorf("read embedded icons: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		embeddedPath := filepath.Join("assets", "icons", entry.Name())
		b, readErr := iconAssets.ReadFile(embeddedPath)
		if readErr != nil {
			return fmt.Errorf("read embedded icon %s: %w", entry.Name(), readErr)
		}
		outPath := filepath.Join(iconsDir, entry.Name())
		if writeErr := os.WriteFile(outPath, b, defaultFilePermission); writeErr != nil {
			return fmt.Errorf("write icon file %s: %w", outPath, writeErr)
		}
	}

	return nil
}
