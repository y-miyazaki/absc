//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.

package resources

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

func TestEventBridgeCollector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "Name", want: "eventbridge_rule"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			collector := &EventBridgeCollector{}
			if got := collector.Name(); got != tt.want {
				t.Fatalf("Name() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewEventBridgeCollector(t *testing.T) {
	t.Parallel()

	cfg := awssdk.Config{Region: "ap-northeast-1"}
	collector, err := NewEventBridgeCollector(&cfg, "us-east-1")
	if err != nil {
		t.Fatalf("NewEventBridgeCollector() error = %v", err)
	}
	if collector == nil {
		t.Fatal("NewEventBridgeCollector() = nil, want collector")
	}
	if got, want := collector.region, "us-east-1"; got != want {
		t.Fatalf("collector.region = %q, want %q", got, want)
	}
}

func TestEventPatternSourceLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		ruleName string
		want     string
	}{
		{name: "empty pattern", pattern: "", ruleName: "my-rule", want: "my-rule"},
		{name: "invalid json", pattern: "{", ruleName: "my-rule", want: "my-rule"},
		{name: "missing source", pattern: `{"detail-type":["Scheduled Event"]}`, ruleName: "my-rule", want: "my-rule"},
		{name: "empty source array", pattern: `{"source":[]}`, ruleName: "my-rule", want: "my-rule"},
		{name: "single source", pattern: `{"source":["aws.ec2"]}`, ruleName: "my-rule", want: "aws.ec2"},
		{name: "multiple sources", pattern: `{"source":["aws.ec2","aws.s3"]}`, ruleName: "my-rule", want: "aws.ec2, aws.s3"},
		{name: "skips empty source values", pattern: `{"source":["aws.lambda","","aws.events"]}`, ruleName: "my-rule", want: "aws.lambda, aws.events"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := eventPatternSourceLabel(tt.pattern, tt.ruleName); got != tt.want {
				t.Fatalf("eventPatternSourceLabel() = %q, want %q", got, tt.want)
			}
		})
	}
}

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
					TaskDefinitionArn: awssdk.String("arn:aws:ecs:ap-northeast-1:123:task-definition/prd-api:31"),
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
