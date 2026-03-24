package runs

import "testing"

func TestIsMeasurableAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetKind   string
		targetAction string
		want         bool
	}{
		{name: "lambda invoke", targetKind: "lambda", targetAction: "lambda:invoke", want: true},
		{name: "lambda direct target", targetKind: "lambda", targetAction: "", want: true},
		{name: "lambda update", targetKind: "lambda", targetAction: "lambda:updateFunctionConfiguration", want: false},
		{name: "ecs run task", targetKind: "ecs", targetAction: "ecs:runTask", want: true},
		{name: "ecs other action", targetKind: "ecs", targetAction: "ecs:updateService", want: false},
		{name: "glue start job run", targetKind: "glue", targetAction: "glue:startJobRun", want: true},
		{name: "stepfunctions start execution", targetKind: "stepfunctions", targetAction: "sfn:startExecution", want: true},
		{name: "stepfunctions other action", targetKind: "stepfunctions", targetAction: "sfn:updateStateMachine", want: false},
		{name: "unknown kind", targetKind: "unknown", targetAction: "unknown:doThing", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isMeasurableAction(tt.targetKind, tt.targetAction); got != tt.want {
				t.Fatalf("isMeasurableAction(%q, %q) = %v, want %v", tt.targetKind, tt.targetAction, got, tt.want)
			}
		})
	}
}

func TestCloudTrailRequestedStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventName string
		want      string
	}{
		{name: "update", eventName: "UpdateFunctionConfiguration", want: "UPDATE_REQUESTED"},
		{name: "delete", eventName: "DeleteFunction", want: "DELETE_REQUESTED"},
		{name: "start", eventName: "StartExecution", want: "START_REQUESTED"},
		{name: "fallback", eventName: "TagResource", want: "ACTION_REQUESTED"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := cloudTrailRequestedStatus(tt.eventName); got != tt.want {
				t.Fatalf("cloudTrailRequestedStatus(%q) = %q, want %q", tt.eventName, got, tt.want)
			}
		})
	}
}
