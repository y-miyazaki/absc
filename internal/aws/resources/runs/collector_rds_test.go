//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestRDSCollector_Service(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		collector *rdsCollector
		want      string
	}{
		{name: "rds", collector: &rdsCollector{}, want: "rds"},
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

func TestRDSCloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(testYear, testMonth, testDay, testHour, testMinute, testSecond, 0, time.UTC)
	since := eventTime.Add(-time.Minute)
	collector := &rdsCollector{}

	tests := []struct {
		name       string
		payload    string
		eventName  string
		wantStatus string
	}{
		{
			name: "start cluster",
			payload: `{
				"eventID":"rds-event-id",
				"requestParameters":{"dBClusterIdentifier":"prd-db-cluster"}
			}`,
			eventName:  "StartDBCluster",
			wantStatus: "START_REQUESTED",
		},
		{
			name: "status from response state",
			payload: `{
				"eventID":"rds-event-id",
				"requestParameters":{"dBClusterIdentifier":"prd-db-cluster"},
				"responseElements":{"status":"stopping"}
			}`,
			eventName:  "StopDBCluster",
			wantStatus: "STOP_REQUESTED",
		},
		{
			name: "start uses target state",
			payload: `{
				"eventID":"rds-event-id",
				"requestParameters":{"dBClusterIdentifier":"prd-db-cluster"},
				"responseElements":{"status":"stopped"}
			}`,
			eventName:  "StartDBCluster",
			wantStatus: "START_REQUESTED",
		},
		{
			name: "stop uses target state",
			payload: `{
				"eventID":"rds-event-id",
				"requestParameters":{"dBClusterIdentifier":"prd-db-cluster"},
				"responseElements":{"status":"available"}
			}`,
			eventName:  "StopDBCluster",
			wantStatus: "STOP_REQUESTED",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := cloudtrailtypes.Event{
				CloudTrailEvent: aws.String(tt.payload),
				EventId:         aws.String("lookup-rds-event-id"),
				EventName:       aws.String(tt.eventName),
				EventTime:       aws.Time(eventTime),
			}

			runs := collector.runsFromEvent(&event, since)
			if got, want := len(runs), 1; got != want {
				t.Fatalf(testFmtLenRuns, got, want)
			}
			if got, want := len(runs[0].resourceIDs), 1; got != want {
				t.Fatalf("len(resourceIDs) = %d, want %d", got, want)
			}
			if got, want := runs[0].resourceIDs[0], "prd-db-cluster"; got != want {
				t.Fatalf("resource id = %q, want %q", got, want)
			}
			if got, want := runs[0].run.Status, tt.wantStatus; got != want {
				t.Fatalf(testFmtStatus, got, want)
			}
		})
	}
}
