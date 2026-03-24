package resources

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

func TestResolveEventBridgeTargetDisplay_NonECS(t *testing.T) {
	t.Parallel()

	targetName, targetID := resolveEventBridgeTargetDisplay(
		&eventbridgetypes.Target{},
		"Lambda",
		"arn:aws:lambda:ap-northeast-1:123456789012:function:my-func",
	)

	if got, want := targetName, "my-func"; got != want {
		t.Fatalf("targetName = %q, want %q", got, want)
	}
	if got, want := targetID, ""; got != want {
		t.Fatalf("targetID = %q, want %q", got, want)
	}
}

func TestResolveEventBridgeTargetDisplay_ECSWithTaskDefinition(t *testing.T) {
	t.Parallel()

	targetName, targetID := resolveEventBridgeTargetDisplay(
		&eventbridgetypes.Target{EcsParameters: &eventbridgetypes.EcsParameters{TaskDefinitionArn: aws.String("arn:aws:ecs:ap-northeast-1:123:task-definition/prd-api:31")}},
		"ECS",
		"arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
	)

	if got, want := targetName, "prd-api:31"; got != want {
		t.Fatalf("targetName = %q, want %q", got, want)
	}
	if got, want := targetID, "prd-cluster"; got != want {
		t.Fatalf("targetID = %q, want %q", got, want)
	}
}

func TestResolveEventBridgeTargetDisplay_ECSWithoutTaskDefinition(t *testing.T) {
	t.Parallel()

	targetName, targetID := resolveEventBridgeTargetDisplay(
		&eventbridgetypes.Target{EcsParameters: &eventbridgetypes.EcsParameters{}},
		"ECS",
		"arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
	)

	if got, want := targetName, "prd-cluster"; got != want {
		t.Fatalf("targetName = %q, want %q", got, want)
	}
	if got, want := targetID, "prd-cluster"; got != want {
		t.Fatalf("targetID = %q, want %q", got, want)
	}
}
