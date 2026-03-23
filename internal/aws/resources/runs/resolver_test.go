package runs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

func TestNewResolver(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(
		"us-east-1",
		&sfn.Client{},
		&batch.Client{},
		&cloudtrail.Client{},
		&ecs.Client{},
		&glue.Client{},
		&cloudwatchlogs.Client{},
	)

	if resolver == nil {
		t.Fatal("NewResolver returned nil")
	}
	if resolver.region != "us-east-1" {
		t.Errorf("region = %q, want %q", resolver.region, "us-east-1")
	}
	if len(resolver.collectors) == 0 {
		t.Fatal("collectors not initialized")
	}
	for _, name := range supportedRunTargetKinds {
		if _, ok := resolver.collectors[name]; !ok {
			t.Errorf("collector %q not found", name)
		}
	}
}

func TestResolver_PopulateScheduleRuns_UnknownTargetKind(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(
		"us-east-1",
		&sfn.Client{},
		&batch.Client{},
		&cloudtrail.Client{},
		&ecs.Client{},
		&glue.Client{},
		&cloudwatchlogs.Client{},
	)

	schedule := &resourcescore.Schedule{
		TargetKind: "unknown_service",
		Runs:       []resourcescore.Run{},
	}

	errRecord := resolver.PopulateScheduleRuns(
		context.Background(),
		schedule,
		"arn:aws:unknown",
		"",
		TargetHints{},
		resourcescore.CollectOptions{},
	)

	if errRecord != nil {
		t.Errorf("expected nil for unknown kind, got %v", errRecord)
	}
	if len(schedule.Runs) != 0 {
		t.Errorf("expected no runs for unknown kind, got %d", len(schedule.Runs))
	}
}

func TestResolver_PopulateScheduleRuns_RunsCapped(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(
		"us-east-1",
		&sfn.Client{},
		&batch.Client{},
		&cloudtrail.Client{},
		&ecs.Client{},
		&glue.Client{},
		&cloudwatchlogs.Client{},
	)

	schedule := &resourcescore.Schedule{
		TargetKind: "batch",
		Runs:       []resourcescore.Run{},
	}

	errRecord := resolver.PopulateScheduleRuns(
		context.Background(),
		schedule,
		"arn:aws:batch:us-east-1:123456789012:job-queue/test-queue",
		"test-job",
		TargetHints{},
		resourcescore.CollectOptions{MaxResults: 0},
	)

	// Should not crash even if batch service is not available
	// The error will be captured in ErrorRecord if batch.Client returns error
	// For this test, we just verify the function returns nil or error record but doesn't panic
	if errRecord != nil && errRecord.Service != "batch" {
		t.Errorf("expected batch service error, got %v", errRecord)
	}
}

func TestResolver_PopulateScheduleRuns_SetRunsCappedFlag(t *testing.T) {
	t.Parallel()

	resolver := NewResolver(
		"us-east-1",
		&sfn.Client{},
		&batch.Client{},
		&cloudtrail.Client{},
		&ecs.Client{},
		&glue.Client{},
		&cloudwatchlogs.Client{},
	)

	schedule := &resourcescore.Schedule{
		TargetKind: "unknown_service",
		Runs:       make([]resourcescore.Run, 10),
		RunsCapped: false,
	}

	// For unknown service, no error and runs should remain unchanged
	errRecord := resolver.PopulateScheduleRuns(
		context.Background(),
		schedule,
		"arn:aws:unknown",
		"",
		TargetHints{},
		resourcescore.CollectOptions{MaxResults: 5},
	)

	if errRecord != nil {
		t.Errorf("expected nil error, got %v", errRecord)
	}
	if schedule.RunsCapped {
		t.Error("unexpected RunsCapped=true for unknown kind")
	}
}
