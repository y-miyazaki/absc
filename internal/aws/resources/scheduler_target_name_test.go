package resources

import (
	"strings"
	"testing"
)

func TestAwsSDKServiceFromARN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		arn  string
		want string
	}{
		{"arn:aws:scheduler:::aws-sdk:rds:startDBCluster", "rds"},
		{"arn:aws:scheduler:::aws-sdk:ec2:startInstances", "ec2"},
		{"arn:aws:scheduler:::aws-sdk:sfn:startExecution", "sfn"},
		{"arn:aws:lambda:ap-northeast-1:123456789012:function:my-func", ""},
	}
	for _, tc := range cases {
		got := awsSDKServiceFromARN(strings.ToLower(tc.arn))
		if got != tc.want {
			t.Errorf("awsSDKServiceFromARN(%q) = %q, want %q", tc.arn, got, tc.want)
		}
	}
}

func TestResolveSchedulerTargetName_RDSClusterIdentifier(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:rds:startDBCluster",
		`{"DbClusterIdentifier":"prd-db"}`,
		"",
	)
	if got != "prd-db" {
		t.Fatalf("target name = %q, want %q", got, "prd-db")
	}
}

func TestResolveSchedulerTargetName_EC2SingleInstanceID(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:ec2:startInstances",
		`{"InstanceIds":["i-0a5f7021e8eee6d2a"]}`,
		"",
	)
	if got != "i-0a5f7021e8eee6d2a" {
		t.Fatalf("target name = %q, want %q", got, "i-0a5f7021e8eee6d2a")
	}
}

func TestResolveSchedulerTargetName_EC2MultipleInstanceIDsFallback(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:ec2:startInstances",
		`{"InstanceIds":["i-aaa","i-bbb"]}`,
		"",
	)
	if got != "startInstances" {
		t.Fatalf("target name = %q, want %q", got, "startInstances")
	}
}

func TestResolveSchedulerTargetName_SFNStateMachine(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:sfn:startExecution",
		`{"StateMachineArn":"arn:aws:states:ap-northeast-1:123:stateMachine:prd-flow"}`,
		"arn:aws:states:ap-northeast-1:123:stateMachine:prd-flow",
	)
	if got != "prd-flow" {
		t.Fatalf("target name = %q, want %q", got, "prd-flow")
	}
}

func TestResolveSchedulerTargetName_LambdaFunctionName(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:lambda:invoke",
		`{"FunctionName":"my-func"}`,
		"my-func",
	)
	if got != "my-func" {
		t.Fatalf("target name = %q, want %q", got, "my-func")
	}
}

func TestResolveSchedulerTargetName_BatchJobName(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:batch:submitJob",
		`{"JobQueue":"arn:aws:batch:ap-northeast-1:123:job-queue/prd-queue","JobName":"prd-job"}`,
		"arn:aws:batch:ap-northeast-1:123:job-queue/prd-queue",
	)
	if got != "prd-job" {
		t.Fatalf("target name = %q, want %q", got, "prd-job")
	}
}

func TestResolveSchedulerTargetName_RedshiftClusterIdentifier(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerTargetName(
		"arn:aws:scheduler:::aws-sdk:redshift:executeStatement",
		`{"ClusterIdentifier":"prd-cluster","Database":"mydb","Sql":"SELECT 1"}`,
		"",
	)
	if got != "prd-cluster" {
		t.Fatalf("target name = %q, want %q", got, "prd-cluster")
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
