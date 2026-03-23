package runs

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

func TestECSCloudTrailRunsFromEvent(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 3, 18, 17, 0, 49, 0, time.UTC)
	event := cloudtrailtypes.Event{
		CloudTrailEvent: aws.String(`{
			"eventID":"0c2ac9ce-51d2-4d55-bf47-aab1402cfc54",
			"userIdentity":{
				"arn":"arn:aws:sts::582064665348:assumed-role/prd-recommend-batch-st-cw-role/7e02e4908f2a387186ba93a70d47232d",
				"sessionContext":{"sessionIssuer":{"arn":"arn:aws:iam::582064665348:role/prd-recommend-batch-st-cw-role"}}
			},
			"requestParameters":{
				"cluster":"arn:aws:ecs:ap-northeast-1:582064665348:cluster/prd-recommend-cluster",
				"startedBy":"events-rule/prd-recommend-batch-even",
				"taskDefinition":"arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td:31"
			},
			"responseElements":{
				"tasks":[{
					"clusterArn":"arn:aws:ecs:ap-northeast-1:582064665348:cluster/prd-recommend-cluster",
					"lastStatus":"PROVISIONING",
					"startedBy":"events-rule/prd-recommend-batch-even",
					"taskArn":"arn:aws:ecs:ap-northeast-1:582064665348:task/prd-recommend-cluster/15be4726daa84b7fb2f3c51d13a79455",
					"taskDefinitionArn":"arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td:31"
				}]
			}
		}`),
		EventTime: aws.Time(eventTime),
	}
	runs := ecsCloudTrailRunsFromEvent(&event, eventTime.Add(-time.Minute))

	if got, want := len(runs), 1; got != want {
		t.Fatalf("len(runs) = %d, want %d", got, want)
	}
	if got, want := runs[0].callerARN, "arn:aws:iam::582064665348:role/prd-recommend-batch-st-cw-role"; got != want {
		t.Fatalf("callerARN = %q, want %q", got, want)
	}
	if got, want := runs[0].startedBy, "events-rule/prd-recommend-batch-even"; got != want {
		t.Fatalf("startedBy = %q, want %q", got, want)
	}
	if got, want := runs[0].run.RunID, "15be4726daa84b7fb2f3c51d13a79455"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
	if got, want := runs[0].run.Status, ecsRunStatusStarted; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := runs[0].run.StartAt, "2026-03-18T17:00:49Z"; got != want {
		t.Fatalf("start_at = %q, want %q", got, want)
	}
}

func TestFilterECSCloudTrailRuns(t *testing.T) {
	t.Parallel()

	allRuns := []ecsCloudTrailRun{
		{
			callerARN:         "arn:aws:iam::582064665348:role/prd-step-functions-role",
			clusterARN:        "arn:aws:ecs:ap-northeast-1:582064665348:cluster/prd-recommend-cluster",
			startedBy:         "AWS Step Functions",
			taskDefinitionARN: "arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td:31",
			run:               resourcescore.Run{RunID: "step-functions", StartAt: "2026-03-18T23:33:54Z", Status: ecsRunStatusStarted, SourceService: "cloudtrail"},
		},
		{
			callerARN:         "arn:aws:iam::582064665348:role/prd-recommend-batch-st-cw-role",
			clusterARN:        "arn:aws:ecs:ap-northeast-1:582064665348:cluster/prd-recommend-cluster",
			startedBy:         "events-rule/prd-recommend-batch-even",
			taskDefinitionARN: "arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td:31",
			run:               resourcescore.Run{RunID: "eventbridge", StartAt: "2026-03-18T17:00:49Z", Status: ecsRunStatusStarted, SourceService: "cloudtrail"},
		},
	}

	hints := &TargetHints{
		ECSRoleARN:           "arn:aws:iam::582064665348:role/prd-recommend-batch-st-cw-role",
		ECSTaskDefinitionARN: "arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td",
	}
	filtered := filterECSCloudTrailRuns(allRuns, "arn:aws:ecs:ap-northeast-1:582064665348:cluster/prd-recommend-cluster", hints, 10)

	if got, want := len(filtered), 1; got != want {
		t.Fatalf("len(filtered) = %d, want %d", got, want)
	}
	if got, want := filtered[0].RunID, "eventbridge"; got != want {
		t.Fatalf("run_id = %q, want %q", got, want)
	}
}

func TestMergeECSRunsPrefersDetailedRuns(t *testing.T) {
	t.Parallel()

	merged := mergeECSRuns(
		[]resourcescore.Run{{RunID: "same-task", StartAt: "2026-03-18T17:00:55Z", EndAt: "2026-03-18T17:05:00Z", Status: "STOPPED", SourceService: "ecs"}},
		[]resourcescore.Run{{RunID: "same-task", StartAt: "2026-03-18T17:00:49Z", Status: ecsRunStatusStarted, SourceService: "cloudtrail"}},
		10,
	)

	if got, want := len(merged), 1; got != want {
		t.Fatalf("len(merged) = %d, want %d", got, want)
	}
	if got, want := merged[0].EndAt, "2026-03-18T17:05:00Z"; got != want {
		t.Fatalf("end_at = %q, want %q", got, want)
	}
	if got, want := merged[0].SourceService, "ecs"; got != want {
		t.Fatalf("source_service = %q, want %q", got, want)
	}
}

func TestNormalizeTaskDefinitionARN(t *testing.T) {
	t.Parallel()

	if got, want := normalizeTaskDefinitionARN("arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td:31"), "arn:aws:ecs:ap-northeast-1:582064665348:task-definition/prd-recommend-batch-td"; got != want {
		t.Fatalf("normalizeTaskDefinitionARN() = %q, want %q", got, want)
	}
}
