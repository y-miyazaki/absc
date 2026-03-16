package exporter

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/y-miyazaki/arc/internal/aws/resources"
)

const (
	outputVersion = "1.0"
	slotMinutes   = 10
)

type Output struct {
	Version     string      `json:"version"`
	GeneratedAt string      `json:"generated_at"`
	AccountID   string      `json:"account_id"`
	Timezone    string      `json:"timezone"`
	Window      Window      `json:"window"`
	Schedules   []Schedule  `json:"schedules"`
	Errors      []ErrRecord `json:"errors"`
}

type Window struct {
	Start       string `json:"start"`
	End         string `json:"end"`
	SlotMinutes int    `json:"slot_minutes"`
}

type Schedule struct {
	ID                         string `json:"id"`
	Service                    string `json:"service"`
	ScheduleName               string `json:"schedule_name"`
	ScheduleExpression         string `json:"schedule_expression"`
	ScheduleExpressionTimezone string `json:"schedule_expression_timezone,omitempty"`
	Enabled                    bool   `json:"enabled"`
	Region                     string `json:"region"`
	TargetARN                  string `json:"target_arn"`
	TargetKind                 string `json:"target_kind"`
	TargetService              string `json:"target_service"`
	TargetName                 string `json:"target_name,omitempty"`
	NextInvocationAt           string `json:"next_invocation_at,omitempty"`
	Slots                      []int  `json:"slots"`
	RunsCapped                 bool   `json:"runs_capped,omitempty"`
	Runs                       []Run  `json:"runs"`
}

type Run struct {
	RunID         string `json:"run_id"`
	Status        string `json:"status"`
	StartAt       string `json:"start_at,omitempty"`
	EndAt         string `json:"end_at,omitempty"`
	DurationSec   *int64 `json:"duration_sec,omitempty"`
	SourceService string `json:"source_service"`
}

type ErrRecord struct {
	Service string `json:"service"`
	Region  string `json:"region"`
	Message string `json:"message"`
}

//go:embed html_template.html
var htmlTemplate string

//go:embed assets/icons/*.svg
var iconAssets embed.FS

func WriteJSON(path string, out Output) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create json file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("encode json: %w", err)
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
			End:         time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).Add(24 * time.Hour).Format(time.RFC3339),
			SlotMinutes: slotMinutes,
		},
		Schedules: make([]Schedule, 0, len(schedules)),
		Errors:    make([]ErrRecord, 0, len(errs)),
	}

	for _, s := range schedules {
		sourceLoc := scheduleSourceLocation(s.ScheduleExpressionTimezone)
		slots := remapSlotsToLocation(s.Slots, now, sourceLoc, loc)
		runs := make([]Run, 0, len(s.Runs))
		for _, r := range s.Runs {
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
	for _, e := range errs {
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
	if len(slots) != 144 {
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
		idx := destinationTime.Hour()*6 + (destinationTime.Minute() / 10)
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

func WriteHTML(path string, out Output) error {
	b, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("marshal json for html: %w", err)
	}

	html := htmlTemplate
	html = strings.ReplaceAll(html, "@@INDEX_TITLE@@", "ABSC Cron Timeline")
	html = strings.ReplaceAll(html, "@@PAYLOAD@@", string(b))

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create html file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(html); err != nil {
		return fmt.Errorf("write html file: %w", err)
	}

	if err := writeIconAssets(filepath.Dir(path)); err != nil {
		return err
	}

	return nil
}

func writeIconAssets(baseDir string) error {
	iconsDir := filepath.Join(baseDir, "assets", "icons")
	if err := os.MkdirAll(iconsDir, 0o750); err != nil {
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
		b, err := iconAssets.ReadFile(embeddedPath)
		if err != nil {
			return fmt.Errorf("read embedded icon %s: %w", entry.Name(), err)
		}
		outPath := filepath.Join(iconsDir, entry.Name())
		if err := os.WriteFile(outPath, b, 0o644); err != nil {
			return fmt.Errorf("write icon file %s: %w", outPath, err)
		}
	}

	return nil
}
