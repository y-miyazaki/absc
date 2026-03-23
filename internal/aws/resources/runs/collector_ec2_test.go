package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestEC2CloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"ec2-event-id",
			"requestParameters":{
				"instancesSet":{"items":[{"instanceId":"i-0abc123"},{"instanceId":"i-0def456"}]}
			}
		}`),
		EventId:   aws.String("lookup-event-id"),
		EventName: aws.String("StartInstances"),
		EventTime: aws.Time(eventTime),
	}

	collector := &ec2Collector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := len(runs[0].resourceIDs), 2; got != want {
		t.Fatalf("len(resourceIDs) = %d, want %d", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "i-0abc123"; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := runs[0].run.RunID, "lookup-event-id"; got != want {
		t.Fatalf("run id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.StartAt, "2026-03-18T17:00:49Z"; got != want {
		t.Fatalf("start at = %q, want %q", got, want)
	}
}

func TestEC2CloudTrailRunsFromEvent_StatusFromResponseState(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
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
	}

	collector := &ec2Collector{}
	runs := collector.runsFromEvent(&event, eventTime.Add(-time.Minute))
	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.Status, "STOP_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}
