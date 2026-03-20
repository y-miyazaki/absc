// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: CloudTrail parser code is intentionally compact.
package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	ecsCloudTrailEventName              = "RunTask"
	ecsCloudTrailLookupMaxResults int32 = 50
	ecsRunStatusStarted                 = "STARTED"
	ecsStoppedTaskRetentionApprox       = time.Hour
	ecsTimelineWindowDuration           = 24 * time.Hour
)

type ecsCloudTrailEventEnvelope struct {
	RequestParameters ecsCloudTrailRequestParameters `json:"requestParameters"`
	UserIdentity      ecsCloudTrailUserIdentity      `json:"userIdentity"`
	//nolint:tagliatelle // CloudTrail event payload uses eventID.
	EventID          string                        `json:"eventID"`
	ResponseElements ecsCloudTrailResponseElements `json:"responseElements"`
}

type ecsCloudTrailRequestParameters struct {
	Cluster        string `json:"cluster"`
	StartedBy      string `json:"startedBy"`
	TaskDefinition string `json:"taskDefinition"`
}

type ecsCloudTrailResponseElements struct {
	Tasks []ecsCloudTrailTask `json:"tasks"`
}

type ecsCloudTrailTask struct {
	ClusterARN        string `json:"clusterArn"`
	LastStatus        string `json:"lastStatus"`
	StartedBy         string `json:"startedBy"`
	TaskARN           string `json:"taskArn"`
	TaskDefinitionARN string `json:"taskDefinitionArn"`
}

type ecsCloudTrailRun struct {
	callerARN         string
	clusterARN        string
	run               resourcescore.Run
	startedBy         string
	taskDefinitionARN string
}

type ecsCloudTrailSessionContext struct {
	SessionIssuer ecsCloudTrailSessionIssuer `json:"sessionIssuer"`
}

type ecsCloudTrailSessionIssuer struct {
	ARN string `json:"arn"`
}

type ecsCloudTrailUserIdentity struct {
	ARN            string                      `json:"arn"`
	SessionContext ecsCloudTrailSessionContext `json:"sessionContext"`
}

func collectECSCloudTrailRuns(ctx context.Context, svc *cloudtrail.Client, since time.Time, caches *runCollectorCaches) ([]ecsCloudTrailRun, error) {
	if caches.ecsCTLoaded {
		return caches.ecsCTRuns, caches.ecsCTErr
	}

	runs, err := lookupECSCloudTrailRuns(ctx, svc, since)
	caches.ecsCTRuns = runs
	if err != nil {
		caches.ecsCTErr = fmt.Errorf("lookup ecs cloudtrail runs: %w", err)
	} else {
		caches.ecsCTErr = nil
	}
	caches.ecsCTLoaded = true
	return runs, caches.ecsCTErr
}

func ecsCloudTrailRequired(since, now time.Time) bool {
	windowEnd := since.Add(ecsTimelineWindowDuration)
	return windowEnd.Before(now.Add(-ecsStoppedTaskRetentionApprox))
}

func filterECSCloudTrailRuns(allRuns []ecsCloudTrailRun, clusterARN string, hints TargetHints, maxResults int) []resourcescore.Run {
	matches := make([]resourcescore.Run, 0, maxResults)
	for runIndex := range allRuns {
		candidate := allRuns[runIndex]
		if candidate.clusterARN != clusterARN {
			continue
		}
		if hints.ECSRoleARN != "" && candidate.callerARN != hints.ECSRoleARN {
			continue
		}
		if hints.ECSStartedBy != "" && candidate.startedBy != hints.ECSStartedBy {
			continue
		}
		if hints.ECSTaskDefinitionARN != "" && normalizeTaskDefinitionARN(candidate.taskDefinitionARN) != normalizeTaskDefinitionARN(hints.ECSTaskDefinitionARN) {
			continue
		}
		matches = append(matches, candidate.run)
		if len(matches) >= maxResults {
			return matches
		}
	}
	return matches
}

func lookupECSCloudTrailRuns(ctx context.Context, svc *cloudtrail.Client, since time.Time) ([]ecsCloudTrailRun, error) {
	if svc == nil {
		return nil, nil
	}

	allRuns := make([]ecsCloudTrailRun, 0)
	windowEnd := since.Add(ecsTimelineWindowDuration)
	input := &cloudtrail.LookupEventsInput{
		EndTime:   aws.Time(windowEnd),
		StartTime: aws.Time(since),
		LookupAttributes: []cloudtrailtypes.LookupAttribute{{
			AttributeKey:   cloudtrailtypes.LookupAttributeKeyEventName,
			AttributeValue: aws.String(ecsCloudTrailEventName),
		}},
		MaxResults: aws.Int32(ecsCloudTrailLookupMaxResults),
	}
	for {
		page, err := svc.LookupEvents(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("cloudtrail lookup events: %w", err)
		}
		for eventIndex := range page.Events {
			allRuns = append(allRuns, ecsCloudTrailRunsFromEvent(&page.Events[eventIndex], since)...)
		}
		if page.NextToken == nil || aws.ToString(page.NextToken) == "" {
			break
		}
		input.NextToken = page.NextToken
	}

	slices.SortStableFunc(allRuns, func(left, right ecsCloudTrailRun) int {
		return ecsRunSortTime(&right.run).Compare(ecsRunSortTime(&left.run))
	})
	return allRuns, nil
}

func ecsCloudTrailRunsFromEvent(event *cloudtrailtypes.Event, since time.Time) []ecsCloudTrailRun {
	if event.CloudTrailEvent == nil || event.EventTime == nil || event.EventTime.Before(since) {
		return nil
	}

	var envelope ecsCloudTrailEventEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(event.CloudTrailEvent)), &envelope); err != nil {
		return nil
	}

	callerARN := strings.TrimSpace(envelope.UserIdentity.SessionContext.SessionIssuer.ARN)
	if callerARN == "" {
		callerARN = strings.TrimSpace(envelope.UserIdentity.ARN)
	}
	baseTaskDefinitionARN := firstNonEmpty(envelope.RequestParameters.TaskDefinition, firstTaskDefinitionARN(envelope.ResponseElements.Tasks))
	baseClusterARN := firstNonEmpty(envelope.RequestParameters.Cluster, firstClusterARN(envelope.ResponseElements.Tasks))
	baseStartedBy := firstNonEmpty(envelope.RequestParameters.StartedBy, firstStartedBy(envelope.ResponseElements.Tasks))

	if len(envelope.ResponseElements.Tasks) == 0 {
		return []ecsCloudTrailRun{{
			callerARN:  callerARN,
			clusterARN: baseClusterARN,
			run: resourcescore.Run{
				RunID:         firstNonEmpty(envelope.EventID, helpers.FormatRFC3339NanoUTC(*event.EventTime)),
				StartAt:       helpers.FormatRFC3339UTC(*event.EventTime),
				SourceService: "cloudtrail",
				Status:        ecsRunStatusStarted,
			},
			startedBy:         baseStartedBy,
			taskDefinitionARN: baseTaskDefinitionARN,
		}}
	}

	runs := make([]ecsCloudTrailRun, 0, len(envelope.ResponseElements.Tasks))
	for taskIndex := range envelope.ResponseElements.Tasks {
		task := envelope.ResponseElements.Tasks[taskIndex]
		runID := helpers.ResourceNameFromARN(task.TaskARN)
		if runID == "" {
			runID = firstNonEmpty(envelope.EventID, helpers.FormatRFC3339NanoUTC(*event.EventTime))
		}
		runs = append(runs, ecsCloudTrailRun{
			callerARN:  callerARN,
			clusterARN: firstNonEmpty(task.ClusterARN, baseClusterARN),
			run: resourcescore.Run{
				RunID:         runID,
				StartAt:       helpers.FormatRFC3339UTC(*event.EventTime),
				SourceService: "cloudtrail",
				Status:        ecsRunStatusStarted,
			},
			startedBy:         firstNonEmpty(task.StartedBy, baseStartedBy),
			taskDefinitionARN: firstNonEmpty(task.TaskDefinitionARN, baseTaskDefinitionARN),
		})
	}
	return runs
}

func ecsRunSortTime(run *resourcescore.Run) time.Time {
	if parsed, err := time.Parse(time.RFC3339, run.StartAt); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, run.EndAt); err == nil {
		return parsed
	}
	return time.Time{}
}

func firstClusterARN(tasks []ecsCloudTrailTask) string {
	for taskIndex := range tasks {
		if tasks[taskIndex].ClusterARN != "" {
			return tasks[taskIndex].ClusterARN
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstStartedBy(tasks []ecsCloudTrailTask) string {
	for taskIndex := range tasks {
		if tasks[taskIndex].StartedBy != "" {
			return tasks[taskIndex].StartedBy
		}
	}
	return ""
}

func firstTaskDefinitionARN(tasks []ecsCloudTrailTask) string {
	for taskIndex := range tasks {
		if tasks[taskIndex].TaskDefinitionARN != "" {
			return tasks[taskIndex].TaskDefinitionARN
		}
	}
	return ""
}

func mergeECSRuns(primaryRuns, fallbackRuns []resourcescore.Run, maxResults int) []resourcescore.Run {
	merged := make([]resourcescore.Run, 0, len(primaryRuns)+len(fallbackRuns))
	seen := make(map[string]struct{}, len(primaryRuns)+len(fallbackRuns))
	appendUnique := func(runs []resourcescore.Run) {
		for runIndex := range runs {
			run := runs[runIndex]
			key := run.RunID
			if key == "" {
				key = run.StartAt + "|" + run.EndAt + "|" + run.SourceService
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, run)
		}
	}
	appendUnique(primaryRuns)
	appendUnique(fallbackRuns)
	slices.SortStableFunc(merged, func(left, right resourcescore.Run) int {
		return ecsRunSortTime(&right).Compare(ecsRunSortTime(&left))
	})
	if len(merged) > maxResults {
		return merged[:maxResults]
	}
	return merged
}

func normalizeTaskDefinitionARN(taskDefinitionARN string) string {
	trimmed := strings.TrimSpace(taskDefinitionARN)
	if trimmed == "" {
		return ""
	}
	lastColon := strings.LastIndex(trimmed, ":")
	if lastColon < 0 || lastColon <= strings.LastIndex(trimmed, "/") {
		return trimmed
	}
	if _, err := fmt.Sscanf(trimmed[lastColon+1:], "%d", new(int)); err == nil {
		return trimmed[:lastColon]
	}
	return trimmed
}
