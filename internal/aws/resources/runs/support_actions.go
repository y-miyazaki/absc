// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: small action-classification helpers are intentionally compact.
package runs

import "strings"

var (
	batchCloudTrailRequestResourceKeys         = []string{"jobQueue", "jobName", "jobId", "jobDefinition"}
	ecsCloudTrailRequestResourceKeys           = []string{"cluster", "service", "taskDefinition"}
	glueCloudTrailRequestResourceKeys          = []string{"jobName"}
	lambdaCloudTrailRequestResourceKeys        = []string{"functionName", "functionArn"}
	rdsCloudTrailRequestResourceKeys           = []string{"dbClusterIdentifier", "dbInstanceIdentifier"}
	redshiftCloudTrailRequestResourceKeys      = []string{"clusterIdentifier"}
	stepFunctionsCloudTrailRequestResourceKeys = []string{"stateMachineArn", "name"}
)

func actionKey(targetAction string) string {
	return strings.ToLower(strings.TrimSpace(targetAction))
}

func isMeasurableAction(targetKind, targetAction string) bool {
	switch strings.ToLower(strings.TrimSpace(targetKind)) {
	case "batch":
		return targetAction == "" || actionKey(targetAction) == "batch:submitjob"
	case "ecs":
		return targetAction == "" || actionKey(targetAction) == "ecs:runtask"
	case "glue":
		return targetAction == "" || actionKey(targetAction) == "glue:startjobrun"
	case "lambda":
		return targetAction == "" || actionKey(targetAction) == "lambda:invoke"
	case "stepfunctions":
		return targetAction == "" || actionKey(targetAction) == "sfn:startexecution"
	default:
		return false
	}
}

func cloudTrailRequestedStatus(eventName string) string {
	event := strings.TrimSpace(eventName)
	switch {
	case strings.HasPrefix(event, "Create"):
		return "CREATE_REQUESTED"
	case strings.HasPrefix(event, "Delete"):
		return "DELETE_REQUESTED"
	case strings.HasPrefix(event, "Modify"), strings.HasPrefix(event, "Put"), strings.HasPrefix(event, "Update"):
		return "UPDATE_REQUESTED"
	case strings.HasPrefix(event, "Pause"), strings.HasPrefix(event, "Stop"):
		return "STOP_REQUESTED"
	case strings.HasPrefix(event, "Reboot"), strings.HasPrefix(event, "Restart"):
		return "REBOOT_REQUESTED"
	case strings.HasPrefix(event, "Resume"), strings.HasPrefix(event, "Run"), strings.HasPrefix(event, "Start"):
		return "START_REQUESTED"
	default:
		return "ACTION_REQUESTED"
	}
}
