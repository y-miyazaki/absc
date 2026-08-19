//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

func TestCloudTrailRunSortTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  resourcescore.Run
		want time.Time
	}{
		{
			name: "start at",
			run:  resourcescore.Run{StartAt: "2026-03-18T01:00:00Z"},
			want: time.Date(2026, 3, 18, 1, 0, 0, 0, time.UTC),
		},
		{
			name: "end at fallback",
			run:  resourcescore.Run{EndAt: "2026-03-18T02:00:00Z"},
			want: time.Date(2026, 3, 18, 2, 0, 0, 0, time.UTC),
		},
		{name: "invalid timestamps", run: resourcescore.Run{}, want: time.Time{}},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := cloudTrailRunSortTime(&tt.run)
			if !got.Equal(tt.want) {
				t.Fatalf("cloudTrailRunSortTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCloudTrailResourceNames(t *testing.T) {
	t.Parallel()

	event := &cloudtrailtypes.Event{
		Resources: []cloudtrailtypes.Resource{
			{ResourceName: aws.String("i-abc123")},
			{ResourceName: aws.String("vol-xyz")},
			{ResourceName: aws.String("i-def456")},
		},
	}

	got := cloudTrailResourceNames(event, "i-")
	want := []string{"i-abc123", "i-def456"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cloudTrailResourceNames() = %v, want %v", got, want)
	}
	if gotNil := cloudTrailResourceNames(nil, "i-"); gotNil != nil {
		t.Fatalf("cloudTrailResourceNames(nil) = %v, want nil", gotNil)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()

	if got, want := firstNonEmpty("", "  ", "value", "later"), "value"; got != want {
		t.Fatalf("firstNonEmpty() = %q, want %q", got, want)
	}
	if got, want := firstNonEmpty("", "   "), ""; got != want {
		t.Fatalf("firstNonEmpty(all empty) = %q, want %q", got, want)
	}
}

func TestCloudTrailRunFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 1, 0, 0, 0, time.UTC)
	event := &cloudtrailtypes.Event{
		EventId:   aws.String("lookup-id"),
		EventTime: aws.Time(eventTime),
	}

	run := cloudTrailRunFromEvent(event, "envelope-id", "START_REQUESTED")
	if got, want := run.RunID, "lookup-id"; got != want {
		t.Fatalf("RunID = %q, want %q", got, want)
	}
	if got, want := run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("Status = %q, want %q", got, want)
	}
	if got, want := run.StartAt, "2026-03-18T01:00:00Z"; got != want {
		t.Fatalf("StartAt = %q, want %q", got, want)
	}
}

func TestCollectCloudTrailActionRuns_EmptyEventName(t *testing.T) {
	t.Parallel()

	runs, err := collectCloudTrailActionRuns(
		t.Context(),
		nil,
		"   ",
		time.Time{},
		time.Time{},
		newRunCollectorCaches(),
		func(*cloudtrailtypes.Event, time.Time) []cloudTrailActionRun { return nil },
	)
	if err != nil {
		t.Fatalf("collectCloudTrailActionRuns() error = %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("len(runs) = %d, want 0", len(runs))
	}
}

func TestFilterCloudTrailActionRuns_EmptyResourceIDs(t *testing.T) {
	t.Parallel()

	got := filterCloudTrailActionRuns([]cloudTrailActionRun{{resourceIDs: []string{"i-1"}}}, nil, 10)
	if len(got) != 0 {
		t.Fatalf("len(runs) = %d, want 0", len(got))
	}
}
