//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.

package runs

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

func TestCloudTrailEventName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetAction string
		want         string
	}{
		{name: "ec2", targetAction: "ec2:startInstances", want: "StartInstances"},
		{name: "rds", targetAction: "rds:startDBCluster", want: "StartDBCluster"},
		{name: "invalid", targetAction: "", want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := cloudTrailEventName(tt.targetAction); got != tt.want {
				t.Fatalf("cloudTrailEventName(%q) = %q, want %q", tt.targetAction, got, tt.want)
			}
		})
	}
}

func TestLookupCloudTrailEvents(t *testing.T) {
	t.Parallel()

	since := time.Date(testYear, testMonth, testDay24, 0, 0, 0, 0, time.UTC)
	caches := newRunCollectorCaches()

	tests := []struct {
		name      string
		eventName string
	}{
		{name: "nil client", eventName: "StartJobRun"},
		{name: "empty event name", eventName: "   "},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			events, err := lookupCloudTrailEvents(context.Background(), nil, tt.eventName, since, time.Time{}, caches)
			if err != nil {
				t.Fatalf("lookupCloudTrailEvents() error = %v", err)
			}
			if events != nil {
				t.Fatalf("lookupCloudTrailEvents() events = %v, want nil", events)
			}
		})
	}
}

func TestFilterCloudTrailActionRuns(t *testing.T) {
	t.Parallel()

	runs := filterCloudTrailActionRuns([]cloudTrailActionRun{
		{resourceIDs: []string{"i-aaa"}},
		{resourceIDs: []string{"i-bbb"}},
	}, []string{"i-bbb"}, testPageLimit10)

	if got, want := len(runs), 1; got != want {
		t.Fatalf(testFmtLenRuns, got, want)
	}
}

func TestCloudTrailResourceIDsFromMap_CaseInsensitive(t *testing.T) {
	t.Parallel()

	got := cloudTrailResourceIDsFromMap(map[string]any{"JobQueue": "queue-a", "JOBNAME": "job-a"}, []string{"jobQueue", "jobName"})
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "queue-a" || got[1] != "job-a" {
		t.Fatalf("resourceIDs = %v, want [queue-a job-a]", got)
	}
}

func TestAppendUniqueTrimmedResourceIDs(t *testing.T) {
	t.Parallel()

	got := appendUniqueTrimmedResourceIDs(nil, " arn:aws:lambda:ap-northeast-1:123456789012:function:my-function ", testMyFunction, "", testMyFunction)
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "arn:aws:lambda:ap-northeast-1:123456789012:function:my-function" || got[1] != testMyFunction {
		t.Fatalf("resourceIDs = %v, want [arn:aws:lambda:ap-northeast-1:123456789012:function:my-function my-function]", got)
	}
}

func TestAppendResourceNameFromARN(t *testing.T) {
	t.Parallel()

	got := appendResourceNameFromARN([]string{"arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample"}, "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample")
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample" || got[1] != "sample" {
		t.Fatalf("resourceIDs = %v, want [arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample sample]", got)
	}
}

func TestCloudTrailRunSortTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want time.Time
		run  resourcescore.Run
		name string
	}{
		{
			name: "start at",
			run:  resourcescore.Run{StartAt: "2026-03-18T01:00:00Z"},
			want: time.Date(testYear, testMonth, testDay, testHour1, 0, 0, 0, time.UTC),
		},
		{
			name: "end at fallback",
			run:  resourcescore.Run{EndAt: "2026-03-18T02:00:00Z"},
			want: time.Date(testYear, testMonth, testDay, testHour2, 0, 0, 0, time.UTC),
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

	eventTime := time.Date(testYear, testMonth, testDay, testHour1, 0, 0, 0, time.UTC)
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

	got := filterCloudTrailActionRuns([]cloudTrailActionRun{{resourceIDs: []string{"i-1"}}}, nil, testPageLimit10)
	if len(got) != 0 {
		t.Fatalf("len(runs) = %d, want 0", len(got))
	}
}

func TestGenericCloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(testYear, testMonth, testDay24, testHour2, 0, 0, 0, time.UTC)
	event := cloudtrailtypes.Event{
		EventName: aws.String("UpdateFunctionConfiguration"),
		EventTime: aws.Time(eventTime),
		CloudTrailEvent: aws.String(`{
			"eventID":"lambda-update-event",
			"requestParameters":{"functionName":"my-function"},
			"responseElements":null
		}`),
	}

	runs := genericCloudTrailRunsFromEvent(
		&event,
		eventTime.Add(-time.Minute),
		[]string{"functionName", "FunctionName", "functionArn", "FunctionArn"},
	)

	if got, want := len(runs), 1; got != want {
		t.Fatalf(testFmtLenRuns, got, want)
	}
	if got, want := runs[0].resourceIDs[0], testMyFunction; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
	if got, want := runs[0].run.RunID, "lambda-update-event"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "UPDATE_REQUESTED"; got != want {
		t.Fatalf(testFmtStatus, got, want)
	}
	if got, want := runs[0].run.StartAt, "2026-03-24T02:00:00Z"; got != want {
		t.Fatalf("start_at = %q, want %q", got, want)
	}
}
