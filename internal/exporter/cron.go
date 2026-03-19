//revive:disable:comments-density reason: data-shaping code is clearer without line-by-line commentary.
package exporter

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/y-miyazaki/absc/internal/aws/resources"
)

const (
	defaultDirPermission  = 0o750
	defaultFilePermission = 0o600
	hoursPerDay           = 24
	hourLabelStep         = 1
	minutesPerSlot        = 10
	outputVersion         = "1.0"
	secondsPerHour        = 3600
	secondsPerMinute      = 60
	slotsPerHour          = 6
	slotsPerTimelineDay   = 144
	slotMinutes           = 10
)

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
	Region                     string `json:"region"`
	TargetName                 string `json:"target_name,omitempty"`
	ScheduleName               string `json:"schedule_name"`
	ScheduleExpression         string `json:"schedule_expression"`
	ScheduleExpressionTimezone string `json:"schedule_expression_timezone,omitempty"`
	ScheduleExpressionTZLabel  string `json:"schedule_expression_timezone_label,omitempty"`
	NextInvocationAt           string `json:"next_invocation_at,omitempty"`
	NextInvocationLabel        string `json:"next_invocation_label,omitempty"`
	Service                    string `json:"service"`
	TargetKind                 string `json:"target_kind"`
	TargetAction               string `json:"target_action,omitempty"`
	ID                         string `json:"id"`
	TargetService              string `json:"target_service"`
	TargetARN                  string `json:"target_arn"`
	Slots                      []int  `json:"slots"`
	Runs                       []Run  `json:"runs"`
	Enabled                    bool   `json:"enabled"`
	RunsCapped                 bool   `json:"runs_capped,omitempty"`
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

//go:embed html_template.html
var htmlTemplate string

//go:embed assets/icons/*.svg
var iconAssets embed.FS

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

func BuildOutput(accountID string, now, since time.Time, loc *time.Location, schedules []resources.Schedule, errs []resources.ErrorRecord) Output {
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

	for scheduleIndex := range schedules {
		s := schedules[scheduleIndex]
		sourceLoc := scheduleSourceLocation(s.ScheduleExpressionTimezone)
		slots := remapSlotsToLocation(s.Slots, dayStart, sourceLoc, loc)
		runs := make([]Run, 0, len(s.Runs))
		for runIndex := range s.Runs {
			r := s.Runs[runIndex]
			startAt := convertRFC3339ToLocation(r.StartAt, loc)
			endAt := convertRFC3339ToLocation(r.EndAt, loc)
			if !runOverlapsWindow(startAt, endAt, dayStart, windowEnd) {
				continue
			}
			runs = append(runs, Run{
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
			RunsCapped:                 s.RunsCapped,
			Runs:                       runs,
		})

		for runIndex := range runs {
			run := runs[runIndex]
			if runOverlapsScheduledSlots(&run, slots, dayStart) {
				continue
			}
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
