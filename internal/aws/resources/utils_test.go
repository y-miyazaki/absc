//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"testing"
	"time"
)

func TestBuildSlots_CronWraparoundHourRange(t *testing.T) {
	t.Parallel()

	slots := buildSlots("cron(0 21-2 * * ? *)")

	for _, idx := range []int{0, 6, 12, 126, 132, 138} {
		if slots[idx] != 1 {
			t.Fatalf("slots[%d] = %d, want 1", idx, slots[idx])
		}
	}
	if slots[18] != 0 {
		t.Fatalf("slots[18] = %d, want 0", slots[18])
	}
}

func TestBuildSlots_CronWraparoundMinuteStepRange(t *testing.T) {
	t.Parallel()

	slots := buildSlots("cron(50-10/10 0 * * ? *)")

	for _, idx := range []int{0, 1, 5} {
		if slots[idx] != 1 {
			t.Fatalf("slots[%d] = %d, want 1", idx, slots[idx])
		}
	}
	if slots[2] != 0 {
		t.Fatalf("slots[2] = %d, want 0", slots[2])
	}
}

func TestDetectTargetKind_AwsSDKRedshift(t *testing.T) {
	t.Parallel()

	got := detectTargetKind("arn:aws:scheduler:::aws-sdk:redshift:pauseCluster", false)
	if got != "redshift" {
		t.Fatalf("target kind = %q, want %q", got, "redshift")
	}
}

func TestDetectTargetKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		arn                    string
		want                   string
		batchParametersPresent bool
	}{
		{name: "aws-sdk step functions", arn: "arn:aws:scheduler:::aws-sdk:sfn:startExecution", want: "stepfunctions"},
		{name: "aws-sdk batch", arn: "arn:aws:scheduler:::aws-sdk:batch:submitJob", want: "batch"},
		{name: "aws-sdk glue", arn: "arn:aws:scheduler:::aws-sdk:glue:startJobRun", want: "glue"},
		{name: "aws-sdk lambda", arn: "arn:aws:scheduler:::aws-sdk:lambda:invoke", want: "lambda"},
		{name: "aws-sdk ecs", arn: "arn:aws:scheduler:::aws-sdk:ecs:runTask", want: "ecs"},
		{name: "direct state machine", arn: "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample", want: "stepfunctions"},
		{name: "direct glue job", arn: "arn:aws:glue:ap-northeast-1:123456789012:job/sample-job", want: "glue"},
		{name: "batch parameters flag", arn: "arn:aws:events:ap-northeast-1:123456789012:rule/sample", batchParametersPresent: true, want: "batch"},
		{name: "unknown", arn: "arn:aws:sns:ap-northeast-1:123456789012:topic/sample", want: "other"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := detectTargetKind(tt.arn, tt.batchParametersPresent)
			if got != tt.want {
				t.Fatalf("detectTargetKind(%q, %v) = %q, want %q", tt.arn, tt.batchParametersPresent, got, tt.want)
			}
		})
	}
}

func TestDetectTargetService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "empty", arn: "  ", want: "Other"},
		{name: "aws-sdk sfn", arn: "arn:aws:scheduler:::aws-sdk:sfn:startExecution", want: "Step Functions"},
		{name: "aws-sdk unknown", arn: "arn:aws:scheduler:::aws-sdk:sns:publish", want: "Other"},
		{name: "direct lambda", arn: "arn:aws:lambda:ap-northeast-1:123456789012:function:sample", want: "Lambda"},
		{name: "direct unknown", arn: "arn:aws:sns:ap-northeast-1:123456789012:topic/sample", want: "Other"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := detectTargetService(tt.arn)
			if got != tt.want {
				t.Fatalf("detectTargetService(%q) = %q, want %q", tt.arn, got, tt.want)
			}
		})
	}
}

func TestParseCronField(t *testing.T) {
	t.Parallel()

	got := parseCronField("0-2", 0, 5)
	want := []int{0, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("len(parseCronField()) = %d, want %d", len(got), len(want))
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("parseCronField()[%d] = %d, want %d", idx, got[idx], want[idx])
		}
	}
}

func TestSafeInt32(t *testing.T) {
	t.Parallel()

	if got, want := safeInt32(42), int32(42); got != want {
		t.Fatalf("safeInt32(42) = %d, want %d", got, want)
	}
	if got, want := safeInt32(-1), int32(0); got != want {
		t.Fatalf("safeInt32(-1) = %d, want %d", got, want)
	}
}

func TestFromMillisHelpers(t *testing.T) {
	t.Parallel()

	if got := fromMillis(0); !got.IsZero() {
		t.Fatalf("fromMillis(0) = %v, want zero", got)
	}

	v := int64(1713571200000)
	if got := fromMillisPtr(&v); got.IsZero() {
		t.Fatal("fromMillisPtr() = zero, want non-zero")
	}
	if got := fromMillisPtr(nil); !got.IsZero() {
		t.Fatalf("fromMillisPtr(nil) = %v, want zero", got)
	}
}

func TestFormatRFC3339Helpers(t *testing.T) {
	t.Parallel()

	zero := formatRFC3339UTC(time.Time{})
	if zero != "" {
		t.Fatalf("formatRFC3339UTC(zero) = %q, want empty", zero)
	}

	nanoZero := formatRFC3339NanoUTC(time.Time{})
	if nanoZero != "" {
		t.Fatalf("formatRFC3339NanoUTC(zero) = %q, want empty", nanoZero)
	}
}

func TestDetectTargetAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "direct resource is empty", arn: "arn:aws:lambda:ap-northeast-1:123456789012:function:sample", want: ""},
		{name: "aws-sdk sfn", arn: "arn:aws:scheduler:::aws-sdk:sfn:startExecution", want: "sfn:startExecution"},
		{name: "aws-sdk incomplete", arn: "arn:aws:scheduler:::aws-sdk:sfn", want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := detectTargetAction(tt.arn)
			if got != tt.want {
				t.Fatalf("detectTargetAction(%q) = %q, want %q", tt.arn, got, tt.want)
			}
		})
	}
}
