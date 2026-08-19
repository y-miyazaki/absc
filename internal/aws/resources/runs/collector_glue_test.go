//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestGlueCollector_Service(t *testing.T) {
	t.Parallel()

	collector := &glueCollector{}
	if got, want := collector.Service(), "glue"; got != want {
		t.Fatalf("Service() = %q, want %q", got, want)
	}
}

func TestGlueCollector_CloudTrailResourceIDs(t *testing.T) {
	t.Parallel()

	collector := &glueCollector{}
	jobARN := "arn:aws:glue:ap-northeast-1:123456789012:job/sample-job"

	if got := collector.cloudTrailResourceIDs("   "); got != nil {
		t.Fatalf("cloudTrailResourceIDs(empty) = %v, want nil", got)
	}

	if got, want := collector.cloudTrailResourceIDs("sample-job"), []string{"sample-job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cloudTrailResourceIDs(name) = %v, want %v", got, want)
	}

	got := collector.cloudTrailResourceIDs(jobARN)
	want := []string{jobARN, "sample-job"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cloudTrailResourceIDs(%q) = %v, want %v", jobARN, got, want)
	}
}

func TestGlueCollector_RunsFromCloudTrailEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 24, 2, 0, 0, 0, time.UTC)
	event := cloudtrailtypes.Event{
		EventName: aws.String("StartJobRun"),
		EventTime: aws.Time(eventTime),
		CloudTrailEvent: aws.String(`{
			"eventID":"glue-start-event",
			"requestParameters":{
				"jobName":"sample-job"
			}
		}`),
	}

	collector := &glueCollector{}
	runs := collector.runsFromCloudTrailEvent(&event, eventTime.Add(-time.Minute))

	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.RunID, "glue-start-event"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "sample-job"; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
}
