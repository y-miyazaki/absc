//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"testing"
	"time"
)

func TestBuildSlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cronExpr string
		wantOn   []int
		wantOff  []int
	}{
		{
			name:     "cron wraparound hour range",
			cronExpr: "cron(0 21-2 * * ? *)",
			wantOn:   []int{0, 6, 12, 126, 132, 138},
			wantOff:  []int{18},
		},
		{
			name:     "cron wraparound minute step range",
			cronExpr: "cron(50-10/10 0 * * ? *)",
			wantOn:   []int{0, 1, 5},
			wantOff:  []int{2},
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slots := buildSlots(tt.cronExpr)
			for _, idx := range tt.wantOn {
				if slots[idx] != 1 {
					t.Fatalf("buildSlots(%q) slots[%d] = %d, want 1", tt.cronExpr, idx, slots[idx])
				}
			}
			for _, idx := range tt.wantOff {
				if slots[idx] != 0 {
					t.Fatalf("buildSlots(%q) slots[%d] = %d, want 0", tt.cronExpr, idx, slots[idx])
				}
			}
		})
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
		{name: "aws-sdk redshift", arn: "arn:aws:scheduler:::aws-sdk:redshift:pauseCluster", want: "redshift"},
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

	tests := []struct {
		name  string
		input int
		want  int32
	}{
		{name: "positive", input: 42, want: 42},
		{name: "negative", input: -1, want: 0},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := safeInt32(tt.input); got != tt.want {
				t.Fatalf("safeInt32(%d) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestFromMillisHelpers(t *testing.T) {
	t.Parallel()

	v := int64(1713571200000)
	tests := []struct {
		millisPtr *int64
		name      string
		millis    int64
		wantZero  bool
	}{
		{name: "zero millis", millis: 0, wantZero: true},
		{name: "with pointer", millisPtr: &v, wantZero: false},
		{name: "nil pointer", millisPtr: nil, wantZero: true},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got time.Time
			switch tt.name {
			case "zero millis":
				got = fromMillis(tt.millis)
			default:
				got = fromMillisPtr(tt.millisPtr)
			}

			if tt.wantZero && !got.IsZero() {
				t.Fatalf("fromMillis helper = %v, want zero", got)
			}
			if !tt.wantZero && got.IsZero() {
				t.Fatal("fromMillis helper = zero, want non-zero")
			}
		})
	}
}

func TestFormatRFC3339Helpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		useNano bool
	}{
		{name: "UTC zero", useNano: false},
		{name: "nano UTC zero", useNano: true},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got string
			if tt.useNano {
				got = formatRFC3339NanoUTC(time.Time{})
			} else {
				got = formatRFC3339UTC(time.Time{})
			}
			if got != "" {
				t.Fatalf("format helper(zero) = %q, want empty", got)
			}
		})
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
