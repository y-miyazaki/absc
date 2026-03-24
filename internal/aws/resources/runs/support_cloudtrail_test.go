package runs

import "testing"

func TestCloudTrailEventName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetAction string
		want         string
	}{
		{name: "ec2", targetAction: "ec2:startInstances", want: "StartInstances"},
		{name: "rds", targetAction: "rds:startDBCluster", want: "StartDBCluster"},
		{name: "invalid", targetAction: "", want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := cloudTrailEventName(tt.targetAction); got != tt.want {
				t.Fatalf("cloudTrailEventName(%q) = %q, want %q", tt.targetAction, got, tt.want)
			}
		})
	}
}

func TestFilterCloudTrailActionRuns(t *testing.T) {
	t.Parallel()

	runs := filterCloudTrailActionRuns([]cloudTrailActionRun{
		{resourceIDs: []string{"i-aaa"}},
		{resourceIDs: []string{"i-bbb"}},
	}, []string{"i-bbb"}, 10)

	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
}

func TestCloudTrailResourceIDsFromMap_CaseInsensitive(t *testing.T) {
	t.Parallel()

	got := cloudTrailResourceIDsFromMap(map[string]any{"JobQueue": "queue-a", "JOBNAME": "job-a"}, []string{"jobQueue", "jobName"})
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "queue-a" || got[1] != "job-a" {
		t.Fatalf("resourceIDs = %v, want [queue-a job-a]", got)
	}
}

func TestAppendUniqueTrimmedResourceIDs(t *testing.T) {
	t.Parallel()

	got := appendUniqueTrimmedResourceIDs(nil, " arn:aws:lambda:ap-northeast-1:123456789012:function:my-function ", "my-function", "", "my-function")
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "arn:aws:lambda:ap-northeast-1:123456789012:function:my-function" || got[1] != "my-function" {
		t.Fatalf("resourceIDs = %v, want [arn:aws:lambda:ap-northeast-1:123456789012:function:my-function my-function]", got)
	}
}

func TestAppendResourceNameFromARN(t *testing.T) {
	t.Parallel()

	got := appendResourceNameFromARN([]string{"arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample"}, "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample")
	if gotLen, wantLen := len(got), 2; gotLen != wantLen {
		t.Fatalf("len(resourceIDs) = %d, want %d", gotLen, wantLen)
	}
	if got[0] != "arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample" || got[1] != "sample" {
		t.Fatalf("resourceIDs = %v, want [arn:aws:states:ap-northeast-1:123456789012:stateMachine:sample sample]", got)
	}
}
