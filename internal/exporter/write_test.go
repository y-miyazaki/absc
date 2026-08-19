//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package exporter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleOutput() *Output {
	return &Output{
		Version:     outputVersion,
		GeneratedAt: time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC).Format(time.RFC3339),
		AccountID:   "123456789012",
		Timezone:    "UTC",
		Window: Window{
			Start:      "2026-03-18T00:00:00Z",
			End:        "2026-03-19T00:00:00Z",
			HourLabels: []string{"00:00"},
			SlotLabels: []string{"00:00 - 00:10"},
		},
		Schedules: []Schedule{{
			ID:           "schedule-1",
			ScheduleName: "sample",
			Region:       "ap-northeast-1",
			TargetService: "Lambda",
			SlotRunIssues: []SlotRunIssue{{
				SlotIndex: 3,
				SlotLabel: "00:30 - 00:40",
				Reason:    slotIssueNoRunInWindow,
			}},
		}},
		Errors: []ErrRecord{{
			Service: "eventbridge_rule",
			Region:  "ap-northeast-1",
			Message: "sample error",
		}},
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "schedules.json")
	if err := WriteJSON(path, sampleOutput()); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(body), `"account_id": "123456789012"`) {
		t.Fatalf("json body missing account_id: %s", body)
	}
}

func TestWriteErrorsHTML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "errors.html")
	if err := WriteErrorsHTML(path, sampleOutput()); err != nil {
		t.Fatalf("WriteErrorsHTML() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(body), "sample error") {
		t.Fatalf("errors html missing message: %s", body)
	}
}

func TestWriteHTML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "index.html")
	if err := WriteHTML(path, sampleOutput()); err != nil {
		t.Fatalf("WriteHTML() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(body), "ABSC Cron Timeline") {
		t.Fatalf("html body missing title: %s", body)
	}
}

func TestWriteSlotRunIssuesCSV(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "slot_run_issues.csv")
	if err := WriteSlotRunIssuesCSV(path, sampleOutput()); err != nil {
		t.Fatalf("WriteSlotRunIssuesCSV() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(body), "NO_RUN_IN_WINDOW") {
		t.Fatalf("csv body missing issue reason: %s", body)
	}
}
