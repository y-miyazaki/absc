package resources

import "testing"

func TestLambdaRunStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "success status maps to completed",
			message: "REPORT RequestId: abc Duration: 12.34 ms Status: success",
			want:    lambdaStatusCompleted,
		},
		{
			name:    "timeout status maps to failed",
			message: "REPORT RequestId: abc Duration: 120000.00 ms Status: timeout",
			want:    lambdaStatusFailed,
		},
		{
			name:    "oom error maps to failed",
			message: "REPORT RequestId: abc Duration: 12.34 ms Status: error Error Type: Runtime.OutOfMemory",
			want:    lambdaStatusFailed,
		},
		{
			name:    "task timed out message maps to failed",
			message: "REPORT RequestId: abc Task timed out after 3.00 seconds",
			want:    lambdaStatusFailed,
		},
		{
			name:    "legacy report without status remains completed",
			message: "REPORT RequestId: abc Duration: 12.34 ms Billed Duration: 100 ms",
			want:    lambdaStatusCompleted,
		},
		{
			name:    "platform report success maps to completed",
			message: `{"time":"2026-03-19T00:00:00.000Z","type":"platform.report","record":{"requestId":"abc","status":"success","metrics":{"durationMs":12.34}}}`,
			want:    lambdaStatusCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambdaRunStatus(tt.message)
			if got != tt.want {
				t.Fatalf("lambdaRunStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLambdaRunStatusDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "status timeout returns timed out detail",
			message: "REPORT RequestId: abc Duration: 120000.00 ms Status: timeout",
			want:    "TIMED_OUT",
		},
		{
			name:    "status error with oom returns out of memory detail",
			message: "REPORT RequestId: abc Duration: 12.34 ms Status: error Error Type: Runtime.OutOfMemory",
			want:    "OUT_OF_MEMORY",
		},
		{
			name:    "status error without known type returns failed detail",
			message: "REPORT RequestId: abc Duration: 12.34 ms Status: error Error Type: Runtime.SomeError",
			want:    lambdaStatusFailed,
		},
		{
			name:    "status success returns completed detail",
			message: "REPORT RequestId: abc Duration: 12.34 ms Status: success",
			want:    lambdaStatusCompleted,
		},
		{
			name:    "platform report timeout returns timed out detail",
			message: `{"time":"2026-03-19T00:00:00.000Z","type":"platform.report","record":{"requestId":"abc","status":"timeout","metrics":{"durationMs":120000}}}`,
			want:    "TIMED_OUT",
		},
		{
			name:    "platform report oom returns out of memory detail",
			message: `{"time":"2026-03-19T00:00:00.000Z","type":"platform.report","record":{"requestId":"abc","status":"error","errorType":"Runtime.OutOfMemory","metrics":{"durationMs":12.34}}}`,
			want:    "OUT_OF_MEMORY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambdaRunStatusDetail(tt.message)
			if got != tt.want {
				t.Fatalf("lambdaRunStatusDetail() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLambdaDurationSec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    int64
	}{
		{
			name:    "plain report rounds up duration seconds",
			message: "REPORT RequestId: abc Duration: 1500.00 ms Status: success",
			want:    2,
		},
		{
			name:    "platform report rounds up duration seconds",
			message: `{"time":"2026-03-19T00:00:00.000Z","type":"platform.report","record":{"requestId":"abc","status":"success","metrics":{"durationMs":1500}}}`,
			want:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := lambdaDurationSec(tt.message)
			if got != tt.want {
				t.Fatalf("lambdaDurationSec() = %d, want %d", got, tt.want)
			}
		})
	}
}
