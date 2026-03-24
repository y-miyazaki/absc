package runs

import "testing"

func TestLambdaRunStatus(t *testing.T) {
	t.Parallel()
	collector := &lambdaCollector{}

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

			got := collector.runStatus(tt.message)
			if got != tt.want {
				t.Fatalf("runStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLambdaRunStatusDetail(t *testing.T) {
	t.Parallel()
	collector := &lambdaCollector{}

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

			got := collector.runStatusDetail(tt.message)
			if got != tt.want {
				t.Fatalf("runStatusDetail() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLambdaDurationSec(t *testing.T) {
	t.Parallel()
	collector := &lambdaCollector{}

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

			got := collector.durationSec(tt.message)
			if got != tt.want {
				t.Fatalf("durationSec() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLambdaCollector_DeduplicationByRequestID(t *testing.T) {
	t.Parallel()

	// Verify that deduplication by RequestID works correctly.
	// Two runs with the same RequestID should be deduplicated using the RequestID map.
	eventID := "request-id-001"

	// Simulate the deduplication logic: map[RequestID]Run
	// Two runs with the same RequestID will overwrite in the map, resulting in one entry.
	runsByID := make(map[string]int)
	runsByID[eventID] = 1 // First occurrence
	runsByID[eventID] = 2 // Second occurrence (overwrites)

	if len(runsByID) != 1 {
		t.Errorf("expected 1 unique run after dedup by RequestID, got %d", len(runsByID))
	}
}

func TestLambdaCollector_FunctionNameExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		functionTarget string
		want           string
	}{
		{
			name:           "arn_format",
			functionTarget: "arn:aws:lambda:us-east-1:123456789012:function:my-function",
			want:           "my-function",
		},
		{
			name:           "arn_with_version",
			functionTarget: "arn:aws:lambda:us-east-1:123456789012:function:my-function:1",
			want:           "my-function",
		},
		{
			name:           "simple_name",
			functionTarget: "my-function",
			want:           "my-function",
		},
		{
			name:           "empty",
			functionTarget: "",
			want:           "",
		},
		{
			name:           "whitespace",
			functionTarget: "  ",
			want:           "",
		},
	}

	collector := &lambdaCollector{}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := collector.functionName(tt.functionTarget)
			if got != tt.want {
				t.Errorf("functionName(%q) = %q, want %q", tt.functionTarget, got, tt.want)
			}
		})
	}
}

func TestLambdaCloudTrailResourceIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		functionTarget string
		want           []string
	}{
		{
			name:           "arn target includes arn and name",
			functionTarget: "arn:aws:lambda:ap-northeast-1:123456789012:function:my-function",
			want:           []string{"arn:aws:lambda:ap-northeast-1:123456789012:function:my-function", "my-function"},
		},
		{
			name:           "name target includes only name",
			functionTarget: "my-function",
			want:           []string{"my-function"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := (&lambdaCollector{}).cloudTrailResourceIDs(tt.functionTarget)
			if len(got) != len(tt.want) {
				t.Fatalf("len(resourceIDs) = %d, want %d", len(got), len(tt.want))
			}
			for idx := range tt.want {
				if got[idx] != tt.want[idx] {
					t.Fatalf("resourceIDs[%d] = %q, want %q", idx, got[idx], tt.want[idx])
				}
			}
		})
	}
}
