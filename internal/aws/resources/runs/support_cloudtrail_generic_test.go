package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestGenericCloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 24, 2, 0, 0, 0, time.UTC)
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
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "my-function"; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
	if got, want := runs[0].run.RunID, "lambda-update-event"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "UPDATE_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := runs[0].run.StartAt, "2026-03-24T02:00:00Z"; got != want {
		t.Fatalf("start_at = %q, want %q", got, want)
	}
}
