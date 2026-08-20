//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestBatchCollector_Service(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		collector *batchCollector
		want      string
	}{
		{name: "batch", collector: &batchCollector{}, want: "batch"},
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

func TestBatchCollector_CloudTrailResourceIDs(t *testing.T) {
	t.Parallel()

	collector := &batchCollector{}
	queueARN := "arn:aws:batch:ap-northeast-1:123456789012:job-queue/sample-queue"

	got := collector.cloudTrailResourceIDs(queueARN, testSampleJob)
	want := []string{queueARN, "sample-queue", testSampleJob}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cloudTrailResourceIDs() = %v, want %v", got, want)
	}

	gotEmpty := collector.cloudTrailResourceIDs("", "")
	if len(gotEmpty) != 0 {
		t.Fatalf("cloudTrailResourceIDs(empty) = %v, want empty", gotEmpty)
	}
}

func TestBatchCollector_RunsFromCloudTrailEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(testYear, testMonth, testDay24, testHour2, 0, 0, 0, time.UTC)
	event := cloudtrailtypes.Event{
		EventName: aws.String("SubmitJob"),
		EventTime: aws.Time(eventTime),
		CloudTrailEvent: aws.String(`{
			"eventID":"batch-submit-event",
			"requestParameters":{
				"jobQueue":"sample-queue",
				"jobName":"sample-job"
			}
		}`),
	}

	collector := &batchCollector{}
	runs := collector.runsFromCloudTrailEvent(&event, eventTime.Add(-time.Minute))

	if got, want := len(runs), 1; got != want {
		t.Fatalf(testFmtLenRuns, got, want)
	}
	if got, want := runs[0].run.RunID, "batch-submit-event"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "ACTION_REQUESTED"; got != want {
		t.Fatalf(testFmtStatus, got, want)
	}
	if got, want := runs[0].resourceIDs[0], "sample-queue"; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
	if got, want := runs[0].resourceIDs[1], testSampleJob; got != want {
		t.Fatalf("resourceIDs[1] = %q, want %q", got, want)
	}
}
