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
	minutesPerSlot        = 10
	outputVersion         = "1.0"
	slotsPerHour          = 6
	slotsPerTimelineDay   = 144
	slotMinutes           = 10
)

//nolint:tagliatelle // Output is a stable external snake_case JSON schema.
type Output struct {
	Version     string      `json:"version"`
	GeneratedAt string      `json:"generated_at"`
	AccountID   string      `json:"account_id"`
	Timezone    string      `json:"timezone"`
	Window      Window      `json:"window"`
	Schedules   []Schedule  `json:"schedules"`
	Errors      []ErrRecord `json:"errors"`
}

//nolint:tagliatelle // Window is a stable external snake_case JSON schema.
type Window struct {
	Start       string `json:"start"`
	End         string `json:"end"`
	SlotMinutes int    `json:"slot_minutes"`
}

//nolint:tagliatelle // Schedule is a stable external snake_case JSON schema.
type Schedule struct {
	Region                     string `json:"region"`
	TargetName                 string `json:"target_name,omitempty"`
	ScheduleName               string `json:"schedule_name"`
	ScheduleExpression         string `json:"schedule_expression"`
	ScheduleExpressionTimezone string `json:"schedule_expression_timezone,omitempty"`
	NextInvocationAt           string `json:"next_invocation_at,omitempty"`
	Service                    string `json:"service"`
	TargetKind                 string `json:"target_kind"`
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
	EndAt         string `json:"end_at,omitempty"`
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

func BuildOutput(accountID string, now time.Time, loc *time.Location, schedules []resources.Schedule, errs []resources.ErrorRecord) Output {
	out := Output{
		Version:     outputVersion,
		GeneratedAt: now.Format(time.RFC3339),
		AccountID:   accountID,
		Timezone:    loc.String(),
		Window: Window{
			Start:       time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).Format(time.RFC3339),
			End:         time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).Add(hoursPerDay * time.Hour).Format(time.RFC3339),
			SlotMinutes: slotMinutes,
		},
		Schedules: make([]Schedule, 0, len(schedules)),
		Errors:    make([]ErrRecord, 0, len(errs)),
	}

	for scheduleIndex := range schedules {
		s := schedules[scheduleIndex]
		sourceLoc := scheduleSourceLocation(s.ScheduleExpressionTimezone)
		slots := remapSlotsToLocation(s.Slots, now, sourceLoc, loc)
		runs := make([]Run, 0, len(s.Runs))
		for runIndex := range s.Runs {
			r := s.Runs[runIndex]
			runs = append(runs, Run{
				RunID:         r.RunID,
				Status:        r.Status,
				StartAt:       convertRFC3339ToLocation(r.StartAt, loc),
				EndAt:         convertRFC3339ToLocation(r.EndAt, loc),
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
			Enabled:                    s.Enabled,
			Region:                     s.Region,
			TargetARN:                  s.TargetARN,
			TargetKind:                 s.TargetKind,
			TargetService:              s.TargetService,
			TargetName:                 s.TargetName,
			NextInvocationAt:           nextInvocationAt,
			Slots:                      slots,
			RunsCapped:                 s.RunsCapped,
			Runs:                       runs,
		})
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
