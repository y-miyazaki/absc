package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestRDSCloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"rds-event-id",
			"requestParameters":{
				"dBClusterIdentifier":"prd-db-cluster"
			}
		}`),
		EventId:   aws.String("lookup-rds-event-id"),
		EventName: aws.String("StartDBCluster"),
		EventTime: aws.Time(eventTime),
	}

	collector := &rdsCollector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := len(runs[0].resourceIDs), 1; got != want {
		t.Fatalf("len(resourceIDs) = %d, want %d", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "prd-db-cluster"; got != want {
		t.Fatalf("resource id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestRDSCloudTrailRunsFromEvent_StatusFromResponseState(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"rds-event-id",
			"requestParameters":{
				"dBClusterIdentifier":"prd-db-cluster"
			},
			"responseElements":{
				"status":"stopping"
			}
		}`),
		EventName: aws.String("StopDBCluster"),
		EventTime: aws.Time(eventTime),
	}

	collector := &rdsCollector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.Status, "STOP_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestRDSCloudTrailRunsFromEvent_StartUsesTargetState(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"rds-event-id",
			"requestParameters":{
				"dBClusterIdentifier":"prd-db-cluster"
			},
			"responseElements":{
				"status":"stopped"
			}
		}`),
		EventName: aws.String("StartDBCluster"),
		EventTime: aws.Time(eventTime),
	}

	collector := &rdsCollector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestRDSCloudTrailRunsFromEvent_StopUsesTargetState(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"rds-event-id",
			"requestParameters":{
				"dBClusterIdentifier":"prd-db-cluster"
			},
			"responseElements":{
				"status":"available"
			}
		}`),
		EventName: aws.String("StopDBCluster"),
		EventTime: aws.Time(eventTime),
	}

	collector := &rdsCollector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.Status, "STOP_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}
