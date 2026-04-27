package runs

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

func TestStepFunctionsCollector_Service(t *testing.T) {
	t.Parallel()

	collector := &stepFunctionsCollector{}
	if got, want := collector.Service(), "stepfunctions"; got != want {
		t.Fatalf("Service() = %q, want %q", got, want)
	}
}

func TestStepFunctionsCollector_CloudTrailResourceIDs(t *testing.T) {
	t.Parallel()

	collector := &stepFunctionsCollector{}
	stateMachineARN := "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample-sm"

	if got := collector.cloudTrailResourceIDs("   "); got != nil {
		t.Fatalf("cloudTrailResourceIDs(empty) = %v, want nil", got)
	}

	got := collector.cloudTrailResourceIDs(stateMachineARN)
	want := []string{stateMachineARN, "sample-sm"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cloudTrailResourceIDs(%q) = %v, want %v", stateMachineARN, got, want)
	}
}

func TestStepFunctionsCollector_IsThrottlingError(t *testing.T) {
	t.Parallel()

	collector := &stepFunctionsCollector{}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "throttling", err: errors.New("ThrottlingException: request throttled"), want: true},
		{name: "rate exceeded", err: errors.New("Rate Exceeded"), want: true},
		{name: "other", err: errors.New("validation error"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := collector.isThrottlingError(tt.err); got != tt.want {
				t.Fatalf("isThrottlingError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestStepFunctionsCollector_RunsFromCloudTrailEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 24, 2, 0, 0, 0, time.UTC)
	event := cloudtrailtypes.Event{
		EventName: aws.String("StartExecution"),
		EventTime: aws.Time(eventTime),
		CloudTrailEvent: aws.String(`{
			"eventID":"sfn-start-event",
			"requestParameters":{
				"stateMachineArn":"arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample-sm",
				"name":"exec-001"
			}
		}`),
	}

	collector := &stepFunctionsCollector{}
	runs := collector.runsFromCloudTrailEvent(&event, eventTime.Add(-time.Minute))

	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].run.RunID, "sfn-start-event"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, "START_REQUESTED"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := runs[0].resourceIDs[0], "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample-sm"; got != want {
		t.Fatalf("resourceIDs[0] = %q, want %q", got, want)
	}
	if got, want := runs[0].resourceIDs[1], "exec-001"; got != want {
		t.Fatalf("resourceIDs[1] = %q, want %q", got, want)
	}
}
