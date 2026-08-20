//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.

package resources

import (
	"reflect"
	"strings"
	"testing"
)

func TestSchedulerCollector_Name(t *testing.T) {
	t.Parallel()

	collector := &SchedulerCollector{}
	if got, want := collector.Name(), "eventbridge_scheduler"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}

func TestExtractAccountIDFromRoleARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		want    string
	}{
		{name: "valid role arn", roleARN: "arn:aws:iam::123456789012:role/sample", want: "123456789012"},
		{name: "empty", roleARN: "", want: ""},
		{name: "invalid", roleARN: "arn:aws:iam", want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractAccountIDFromRoleARN(tt.roleARN); got != tt.want {
				t.Fatalf("extractAccountIDFromRoleARN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStringFromJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
		keys []string
	}{
		{name: "empty raw", raw: "  ", keys: []string{"key"}, want: ""},
		{name: "invalid json", raw: "{", keys: []string{"key"}, want: ""},
		{name: "first matching key", raw: `{"JobName":"sample","Other":"x"}`, keys: []string{"Missing", "JobName"}, want: "sample"},
		{name: "non-string value", raw: `{"count":1}`, keys: []string{"count"}, want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := getStringFromJSON(tt.raw, tt.keys...); got != tt.want {
				t.Fatalf("getStringFromJSON() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStringSliceFromJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		keys []string
		want []string
	}{
		{name: "empty raw", raw: "", keys: []string{"InstanceIds"}, want: nil},
		{name: "invalid json", raw: "{", keys: []string{"InstanceIds"}, want: nil},
		{name: "string slice", raw: `{"InstanceIds":["i-aaa","i-bbb"]}`, keys: []string{"InstanceIds"}, want: []string{"i-aaa", "i-bbb"}},
		{name: "skips empty strings", raw: `{"InstanceIds":["i-aaa","  "]}`, keys: []string{"InstanceIds"}, want: []string{"i-aaa"}},
		{name: "non-array value", raw: `{"InstanceIds":"i-aaa"}`, keys: []string{"InstanceIds"}, want: nil},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getStringSliceFromJSON(tt.raw, tt.keys...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("getStringSliceFromJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveSchedulerRunTarget_BatchFallback(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerRunTarget(
		"arn:aws:batch:ap-northeast-1:123456789012:job-queue/sample",
		"",
	)
	if !got.hasBatchParameters {
		t.Fatal("hasBatchParameters = false, want true")
	}
}

func TestAwsSDKServiceFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "rds", arn: "arn:aws:scheduler:::aws-sdk:rds:startDBCluster", want: "rds"},
		{name: "ec2", arn: "arn:aws:scheduler:::aws-sdk:ec2:startInstances", want: "ec2"},
		{name: "sfn", arn: "arn:aws:scheduler:::aws-sdk:sfn:startExecution", want: "sfn"},
		{name: "non sdk arn", arn: "arn:aws:lambda:ap-northeast-1:123456789012:function:my-func", want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := awsSDKServiceFromARN(strings.ToLower(tt.arn))
			if got != tt.want {
				t.Fatalf("awsSDKServiceFromARN(%q) = %q, want %q", tt.arn, got, tt.want)
			}
		})
	}
}

func TestResolveSchedulerTargetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetARN    string
		input        string
		runTargetARN string
		want         string
	}{
		{
			name:      "rds cluster identifier",
			targetARN: "arn:aws:scheduler:::aws-sdk:rds:startDBCluster",
			input:     `{"DbClusterIdentifier":"prd-db"}`,
			want:      "prd-db",
		},
		{
			name:      "ec2 single instance id",
			targetARN: "arn:aws:scheduler:::aws-sdk:ec2:startInstances",
			input:     `{"InstanceIds":["i-0a5f7021e8eee6d2a"]}`,
			want:      "i-0a5f7021e8eee6d2a",
		},
		{
			name:      "ec2 multiple instance ids fallback",
			targetARN: "arn:aws:scheduler:::aws-sdk:ec2:startInstances",
			input:     `{"InstanceIds":["i-aaa","i-bbb"]}`,
			want:      "startInstances",
		},
		{
			name:         "sfn state machine",
			targetARN:    "arn:aws:scheduler:::aws-sdk:sfn:startExecution",
			input:        `{"StateMachineArn":"arn:aws:states:ap-northeast-1:123:stateMachine:prd-flow"}`,
			runTargetARN: "arn:aws:states:ap-northeast-1:123:stateMachine:prd-flow",
			want:         "prd-flow",
		},
		{
			name:         "lambda function name",
			targetARN:    "arn:aws:scheduler:::aws-sdk:lambda:invoke",
			input:        `{"FunctionName":"my-func"}`,
			runTargetARN: "my-func",
			want:         "my-func",
		},
		{
			name:         "batch job name",
			targetARN:    "arn:aws:scheduler:::aws-sdk:batch:submitJob",
			input:        `{"JobQueue":"arn:aws:batch:ap-northeast-1:123:job-queue/prd-queue","JobName":"prd-job"}`,
			runTargetARN: "arn:aws:batch:ap-northeast-1:123:job-queue/prd-queue",
			want:         "prd-job",
		},
		{
			name:         "ecs service and task definition",
			targetARN:    "arn:aws:scheduler:::aws-sdk:ecs:runTask",
			input:        `{"Cluster":"arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster","Service":"prd-api","TaskDefinition":"arn:aws:ecs:ap-northeast-1:123:task-definition/prd-api:31"}`,
			runTargetARN: "arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
			want:         "prd-api (prd-api:31)",
		},
		{
			name:         "ecs service only",
			targetARN:    "arn:aws:scheduler:::aws-sdk:ecs:runTask",
			input:        `{"Cluster":"arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster","Service":"prd-api"}`,
			runTargetARN: "arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
			want:         "prd-api",
		},
		{
			name:         "ecs task definition only",
			targetARN:    "arn:aws:scheduler:::aws-sdk:ecs:runTask",
			input:        `{"Cluster":"arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster","TaskDefinition":"arn:aws:ecs:ap-northeast-1:123:task-definition/prd-api:31"}`,
			runTargetARN: "arn:aws:ecs:ap-northeast-1:123:cluster/prd-cluster",
			want:         "prd-api:31",
		},
		{
			name:      "redshift cluster identifier",
			targetARN: "arn:aws:scheduler:::aws-sdk:redshift:executeStatement",
			input:     `{"ClusterIdentifier":"prd-cluster","Database":"mydb","Sql":"SELECT 1"}`,
			want:      "prd-cluster",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := resolveSchedulerTargetName(tt.targetARN, tt.input, tt.runTargetARN)
			if got != tt.want {
				t.Fatalf("resolveSchedulerTargetName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveSchedulerRunTarget_RedshiftClusterIdentifier(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerRunTarget(
		"arn:aws:scheduler:::aws-sdk:redshift:pauseCluster",
		`{"ClusterIdentifier":"prd-cluster"}`,
	)

	if len(got.hints.RedshiftClusterIDs) != 1 || got.hints.RedshiftClusterIDs[0] != "prd-cluster" {
		t.Fatalf("RedshiftClusterIDs = %#v, want [\"prd-cluster\"]", got.hints.RedshiftClusterIDs)
	}
}
