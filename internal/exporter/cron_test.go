package exporter

import (
	"testing"
	"time"

	"github.com/y-miyazaki/absc/internal/aws/resources"
)

func TestBuildOutput_WindowAnchoredToSinceDay(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("time.LoadLocation() error = %v", err)
	}

	now := time.Date(2026, 3, 19, 11, 30, 0, 0, loc)
	since := now.Add(-24 * time.Hour)

	out := BuildOutput("123456789012", now, since, loc, nil, nil)

	wantStart := time.Date(2026, 3, 18, 0, 0, 0, 0, loc).Format(time.RFC3339)
	wantEnd := time.Date(2026, 3, 19, 0, 0, 0, 0, loc).Format(time.RFC3339)

	if out.Window.Start != wantStart {
		t.Fatalf("window.start = %q, want %q", out.Window.Start, wantStart)
	}
	if out.Window.End != wantEnd {
		t.Fatalf("window.end = %q, want %q", out.Window.End, wantEnd)
	}
}

func TestBuildOutput_PopulatesDisplayLabels(t *testing.T) {
	t.Parallel()

	loc := time.UTC
	now := time.Date(2026, 3, 19, 11, 30, 0, 0, loc)
	since := now.Add(-24 * time.Hour)

	out := BuildOutput(
		"123456789012",
		now,
		since,
		loc,
		[]resources.Schedule{{
			ScheduleName:               "example-schedule",
			ScheduleExpression:         "cron(30 0-12 * * ? *)",
			ScheduleExpressionTimezone: "UTC",
			NextInvocationAt:           "2026-03-19T05:30:00Z",
			Service:                    "eventbridge_scheduler",
			TargetKind:                 "stepfunctions",
			ID:                         "id-1",
			TargetService:              "Step Functions",
			TargetARN:                  "arn:aws:scheduler:::aws-sdk:sfn:startExecution",
			Enabled:                    true,
			Slots:                      make([]int, 144),
			Runs: []resources.Run{{
				RunID:         "run-1",
				Status:        "SUCCEEDED",
				StartAt:       "2026-03-18T00:30:30Z",
				EndAt:         "2026-03-18T00:37:19Z",
				SourceService: "stepfunctions",
			}},
		}},
		nil,
	)

	if got, want := len(out.Window.HourLabels), 24; got != want {
		t.Fatalf("len(window.hour_labels) = %d, want %d", got, want)
	}
	if got, want := len(out.Window.SlotLabels), 144; got != want {
		t.Fatalf("len(window.slot_labels) = %d, want %d", got, want)
	}
	if got, want := out.Window.SlotLabels[3], "00:30 - 00:40"; got != want {
		t.Fatalf("window.slot_labels[3] = %q, want %q", got, want)
	}

	if got, want := out.Schedules[0].NextInvocationLabel, "2026-03-19 05:30:00 UTC"; got != want {
		t.Fatalf("schedule.next_invocation_label = %q, want %q", got, want)
	}
	if got, want := out.Schedules[0].ScheduleExpressionTZLabel, "UTC (UTC+00:00)"; got != want {
		t.Fatalf("schedule.schedule_expression_timezone_label = %q, want %q", got, want)
	}
	if got, want := out.Schedules[0].Runs[0].StartLabel, "2026-03-18 00:30:30 UTC"; got != want {
		t.Fatalf("run.start_label = %q, want %q", got, want)
	}
	if got, want := out.Schedules[0].Runs[0].EndLabel, "2026-03-18 00:37:19 UTC"; got != want {
		t.Fatalf("run.end_label = %q, want %q", got, want)
	}
}

func TestBuildOutput_FiltersRunsOutsideWindow(t *testing.T) {
	t.Parallel()

	loc := time.UTC
	now := time.Date(2026, 3, 19, 5, 10, 0, 0, loc)
	since := now.Add(-24 * time.Hour)

	out := BuildOutput(
		"123456789012",
		now,
		since,
		loc,
		[]resources.Schedule{{
			ScheduleName:               "window-check-schedule",
			ScheduleExpression:         "cron(0 0-10 * * ? *)",
			ScheduleExpressionTimezone: "UTC",
			Service:                    "eventbridge_scheduler",
			TargetKind:                 "stepfunctions",
			ID:                         "id-window",
			TargetService:              "Step Functions",
			TargetARN:                  "arn:aws:scheduler:::aws-sdk:sfn:startExecution",
			Enabled:                    true,
			Slots: func() []int {
				slots := make([]int, 144)
				slots[36] = 1 // 06:00 - 06:10
				return slots
			}(),
			Runs: []resources.Run{
				{RunID: "in-window", Status: "SUCCEEDED", StartAt: "2026-03-18T06:00:49Z", EndAt: "2026-03-18T06:01:07Z", SourceService: "stepfunctions"},
				{RunID: "out-window-late", Status: "SUCCEEDED", StartAt: "2026-03-19T00:00:49Z", EndAt: "2026-03-19T00:01:00Z", SourceService: "stepfunctions"},
				{RunID: "out-window-early", Status: "SUCCEEDED", StartAt: "2026-03-17T23:50:00Z", EndAt: "2026-03-17T23:59:59Z", SourceService: "stepfunctions"},
			},
		}},
		nil,
	)

	if got, want := len(out.Schedules), 1; got != want {
		t.Fatalf("len(schedules) = %d, want %d", got, want)
	}
	if got, want := len(out.Schedules[0].Runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := out.Schedules[0].Runs[0].RunID, "in-window"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := len(out.Alignment), 0; got != want {
		t.Fatalf("len(alignment_issues) = %d, want %d", got, want)
	}
}

func TestBuildOutput_ReportsRunAlignmentIssues(t *testing.T) {
	t.Parallel()

	loc := time.UTC
	now := time.Date(2026, 3, 19, 1, 0, 0, 0, loc)
	since := now.Add(-24 * time.Hour)

	out := BuildOutput(
		"123456789012",
		now,
		since,
		loc,
		[]resources.Schedule{{
			ScheduleName:               "misaligned-schedule",
			ScheduleExpression:         "cron(0 0 * * ? *)",
			ScheduleExpressionTimezone: "UTC",
			Service:                    "eventbridge_scheduler",
			TargetKind:                 "stepfunctions",
			ID:                         "id-misaligned",
			TargetService:              "Step Functions",
			TargetARN:                  "arn:aws:scheduler:::aws-sdk:sfn:startExecution",
			Enabled:                    true,
			Slots:                      append([]int{1}, make([]int, 143)...),
			Runs: []resources.Run{{
				RunID:         "run-misaligned",
				Status:        "SUCCEEDED",
				StartAt:       "2026-03-18T00:20:00Z",
				EndAt:         "2026-03-18T00:21:00Z",
				SourceService: "stepfunctions",
			}},
		}},
		nil,
	)

	if got, want := len(out.Alignment), 1; got != want {
		t.Fatalf("len(alignment_issues) = %d, want %d", got, want)
	}
	if got, want := out.Alignment[0].ScheduleName, "misaligned-schedule"; got != want {
		t.Fatalf("alignment_issues[0].schedule_name = %q, want %q", got, want)
	}
	if got, want := out.Alignment[0].RunID, "run-misaligned"; got != want {
		t.Fatalf("alignment_issues[0].run_id = %q, want %q", got, want)
	}
}

func TestBuildOutput_RemapSlotsFromScheduleTimezoneToUTC(t *testing.T) {
	t.Parallel()

	loc := time.UTC
	now := time.Date(2026, 3, 19, 1, 0, 0, 0, loc)
	since := now.Add(-24 * time.Hour)

	jstSlots := make([]int, 144)
	// 09:00 JST should be rendered at 00:00 UTC.
	jstSlots[9*6] = 1

	out := BuildOutput(
		"123456789012",
		now,
		since,
		loc,
		[]resources.Schedule{{
			ScheduleName:               "jst-schedule",
			ScheduleExpression:         "cron(0 9 * * ? *)",
			ScheduleExpressionTimezone: "Asia/Tokyo",
			Service:                    "eventbridge_scheduler",
			TargetKind:                 "lambda",
			ID:                         "id-jst",
			TargetService:              "Lambda",
			TargetARN:                  "arn:aws:lambda:ap-northeast-1:123456789012:function:example",
			Enabled:                    true,
			Slots:                      jstSlots,
			Runs:                       nil,
		}},
		nil,
	)

	if got, want := len(out.Schedules), 1; got != want {
		t.Fatalf("len(schedules) = %d, want %d", got, want)
	}

	gotSlots := out.Schedules[0].Slots
	if got, want := gotSlots[0], 1; got != want {
		t.Fatalf("slots[0] = %d, want %d", got, want)
	}
	if got, want := out.Schedules[0].ScheduleExpressionTZLabel, "Asia/Tokyo (UTC+09:00)"; got != want {
		t.Fatalf("schedule_expression_timezone_label = %q, want %q", got, want)
	}
	if got, want := gotSlots[9*6], 0; got != want {
		t.Fatalf("slots[54] = %d, want %d", got, want)
	}
}
