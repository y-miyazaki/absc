//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestEC2CloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(testYear, testMonth, testDay, testHour, testMinute, testSecond, 0, time.UTC)

	tests := []struct {
		name       string
		event      cloudtrailtypes.Event
		wantStatus string
		wantRunID  string
		wantIDs    int
	}{
		{
			name: "start instances",
			event: cloudtrailtypes.Event{
				CloudTrailEvent: aws.String(`{
					"eventID":"ec2-event-id",
					"requestParameters":{
						"instancesSet":{"items":[{"instanceId":"i-0abc123"},{"instanceId":"i-0def456"}]}
					}
				}`),
				EventId:   aws.String("lookup-event-id"),
				EventName: aws.String("StartInstances"),
				EventTime: aws.Time(eventTime),
			},
			wantStatus: "START_REQUESTED",
			wantRunID:  "lookup-event-id",
			wantIDs:    2,
		},
		{
			name: "stop instances uses response state",
			event: cloudtrailtypes.Event{
				CloudTrailEvent: aws.String(`{
					"eventID":"ec2-event-id",
					"requestParameters":{
						"instancesSet":{"items":[{"instanceId":"i-0abc123"}]}
					},
					"responseElements":{
						"instancesSet":{"items":[{"currentState":{"name":"stopping"}}]}
					}
				}`),
				EventName: aws.String("StopInstances"),
				EventTime: aws.Time(eventTime),
			},
			wantStatus: "STOP_REQUESTED",
			wantRunID:  "",
			wantIDs:    1,
		},
	}

	collector := &ec2Collector{}
	since := eventTime.Add(-time.Minute)

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runs := collector.runsFromEvent(&tt.event, since)
			if got, want := len(runs), 1; got != want {
				t.Fatalf(testFmtLenRuns, got, want)
			}
			if got, want := len(runs[0].resourceIDs), tt.wantIDs; got != want {
				t.Fatalf("len(resourceIDs) = %d, want %d", got, want)
			}
			if got, want := runs[0].run.Status, tt.wantStatus; got != want {
				t.Fatalf(testFmtStatus, got, want)
			}
			if tt.wantRunID != "" {
				if got, want := runs[0].run.RunID, tt.wantRunID; got != want {
					t.Fatalf("run id = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestEC2Collector_Service(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		collector *ec2Collector
		want      string
	}{
		{name: "ec2", collector: &ec2Collector{}, want: "ec2"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.collector.Service(); got != tt.want {
				t.Fatalf("Service() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEC2RunsFromEvent_EdgeCases(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(testYear, testMonth, testDay, testHour, testMinute, testSecond, 0, time.UTC)
	collector := &ec2Collector{}

	tests := []struct {
		since     time.Time
		name      string
		payload   string
		wantStart string
		wantNil   bool
	}{
		{
			name:    "skips before since",
			payload: `{"eventID":"ec2-event-id","requestParameters":{"instancesSet":{"items":[{"instanceId":"i-0abc123"}]}}}`,
			since:   eventTime.Add(time.Minute),
			wantNil: true,
		},
		{
			name:    "invalid payload",
			payload: `{`,
			since:   eventTime.Add(-time.Minute),
			wantNil: true,
		},
		{
			name:    "empty resource ids",
			payload: `{"eventID":"ec2-event-id","requestParameters":{"instancesSet":{"items":[]}}}`,
			since:   eventTime.Add(-time.Minute),
			wantNil: true,
		},
		{
			name:      "start at format",
			payload:   `{"eventID":"ec2-event-id","requestParameters":{"instancesSet":{"items":[{"instanceId":"i-0abc123"}]}}}`,
			since:     eventTime.Add(-time.Minute),
			wantStart: "2026-03-18T17:00:49Z",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := cloudtrailtypes.Event{
				CloudTrailEvent: aws.String(tt.payload),
				EventTime:       aws.Time(eventTime),
			}
			runs := collector.runsFromEvent(&event, tt.since)
			if tt.wantNil {
				if runs != nil {
					t.Fatalf("runs = %v, want nil", runs)
				}
				return
			}
			if got, want := runs[0].run.StartAt, tt.wantStart; got != want {
				t.Fatalf("start at = %q, want %q", got, want)
			}
		})
	}
}
