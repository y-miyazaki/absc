//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

func TestResolveEventBridgeTargetDisplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		target        *eventbridgetypes.Target
		targetService string
		targetARN     string
		wantName      string
		wantID        string
	}{
		{
			name:          "non ecs target",
			target:        &eventbridgetypes.Target{},
			targetService: "Lambda",
			targetARN:     "arn:aws:lambda:ap-northeast-1:123456789012:function:my-func",
			wantName:      "my-func",
			wantID:        "",
		},
		{
			name: "ecs with task definition",
			target: &eventbridgetypes.Target{
				EcsParameters: &eventbridgetypes.EcsParameters{
					TaskDefinitionArn: aws.String("arn:aws:ecs:ap-northeast-1:123:task-definition/prd-api:31"),
				},
			},
			targetService: "ECS",
			targetARN:     "arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
			wantName:      "prd-api:31",
			wantID:        "prd-cluster",
		},
		{
			name:          "ecs without task definition",
			target:        &eventbridgetypes.Target{EcsParameters: &eventbridgetypes.EcsParameters{}},
			targetService: "ECS",
			targetARN:     "arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
			wantName:      "prd-cluster",
			wantID:        "prd-cluster",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotName, gotID := resolveEventBridgeTargetDisplay(tt.target, tt.targetService, tt.targetARN)
			if gotName != tt.wantName {
				t.Fatalf("targetName = %q, want %q", gotName, tt.wantName)
			}
			if gotID != tt.wantID {
				t.Fatalf("targetID = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}
