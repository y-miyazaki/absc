package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestRedshiftCollector_Service(t *testing.T) {
	t.Parallel()

	collector := &redshiftCollector{}
	if got, want := collector.Service(), "redshift"; got != want {
		t.Fatalf("Service() = %q, want %q", got, want)
	}
}

func TestRedshiftCollector_RunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"redshift-event-id",
			"requestParameters":{
				"clusterIdentifier":"prd-redshift-cluster"
			}
		}`),
		EventName: aws.String("PauseCluster"),
		EventTime: aws.Time(eventTime),
	}

	collector := &redshiftCollector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))

	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "prd-redshift-cluster"; got != want {
		t.Fatalf("resource id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.RunID, "redshift-event-id"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "STOP_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}
