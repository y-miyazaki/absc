//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
)

func TestEventBridgeCollector_Name(t *testing.T) {
	t.Parallel()

	collector := &EventBridgeCollector{}
	if got, want := collector.Name(), "eventbridge_rule"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
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
