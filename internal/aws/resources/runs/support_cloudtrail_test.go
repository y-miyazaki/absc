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
