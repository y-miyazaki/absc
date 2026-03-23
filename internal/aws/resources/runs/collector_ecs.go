// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: collector code is intentionally linear and concise.
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
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
	"github.com/y-miyazaki/absc/internal/helpers"
)

const (
	ecsCloudTrailEventName         = "RunTask"
	ecsRunStatusStarted            = "STARTED"
	ecsStoppedTaskRetentionApprox  = time.Hour
	ecsTimelineWindowDuration      = 24 * time.Hour
	ecsDescribeTasksBatchSize      = 100
	ecsListTasksResourceMultiplier = 2 // Account for both TaskARN collection and subsequent DescribeTasks batch
)

type ecsCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
	ecsSvc *ecs.Client
}

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

func newECSCollector(ecsSvc *ecs.Client, ctSvc *cloudtrail.Client, caches *runCollectorCaches) *ecsCollector {
	return &ecsCollector{caches: caches, ctSvc: ctSvc, ecsSvc: ecsSvc}
}

func (*ecsCollector) Service() string { return "ecs" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *ecsCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = schedule
	_ = runJobName
	cacheKey := targetARN + cacheKeySeparator + hints.ECSTaskDefinitionARN + cacheKeySeparator + hints.ECSStartedBy + cacheKeySeparator + hints.ECSRoleARN
	description := fmt.Sprintf("ECS cluster=%s taskDef=%s startedBy=%s", helpers.ResourceNameFromARN(targetARN), helpers.ResourceNameFromARN(hints.ECSTaskDefinitionARN), hints.ECSStartedBy)
	runs, err := getCachedRunsForCollector(c.caches, c, cacheKey, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, targetARN, &hints, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect ecs runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func (c *ecsCollector) collectRuns(ctx context.Context, clusterARN string, hints *TargetHints, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	ecsRuns, ecsErr := c.collectRunsFromAPI(ctx, clusterARN, hints.ECSTaskDefinitionARN, hints.ECSStartedBy, since, until, maxResults)
	if !ecsCloudTrailRequired(since, time.Now().UTC()) {
		if ecsErr != nil {
			return nil, fmt.Errorf("collect ecs runs from api: %w", ecsErr)
		}
		return ecsRuns, nil
	}

	cloudTrailRuns, cloudTrailErr := c.collectCloudTrailRuns(ctx, since)
	filteredCloudTrailRuns := filterECSCloudTrailRuns(cloudTrailRuns, clusterARN, hints, maxResults)
	mergedRuns := mergeECSRuns(ecsRuns, filteredCloudTrailRuns, maxResults)

	switch {
	case ecsErr == nil && cloudTrailErr == nil:
		return mergedRuns, nil
	case ecsErr == nil && len(mergedRuns) > 0:
		return mergedRuns, nil
	case cloudTrailErr == nil && len(mergedRuns) > 0:
		return mergedRuns, nil
	case ecsErr != nil && cloudTrailErr != nil:
		return nil, fmt.Errorf("collect ecs runs from api and cloudtrail: %w; %w", ecsErr, cloudTrailErr)
	case ecsErr != nil:
		return nil, fmt.Errorf("collect ecs runs from api: %w", ecsErr)
	default:
		return nil, fmt.Errorf("collect ecs runs from cloudtrail: %w", cloudTrailErr)
	}
}

func (c *ecsCollector) collectRunsFromAPI(ctx context.Context, clusterARN, taskDefinitionARN, startedBy string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	var taskARNs []string
	listPageSize := pageSizeForLimit(maxResults*ecsListTasksResourceMultiplier, ecsListTasksPageSizeMax)
	for _, desired := range []ecstypes.DesiredStatus{ecstypes.DesiredStatusStopped, ecstypes.DesiredStatusRunning} {
		var nextToken *string
		for {
			out, err := c.ecsSvc.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String(clusterARN), DesiredStatus: desired, MaxResults: &listPageSize, NextToken: nextToken})
			if err != nil {
				return nil, fmt.Errorf("list ecs tasks for cluster %s: %w", clusterARN, err)
			}
			taskARNs = append(taskARNs, out.TaskArns...)
			if out.NextToken == nil || len(taskARNs) >= maxResults*2 {
				break
			}
			nextToken = out.NextToken
		}
	}
	if len(taskARNs) == 0 {
		return make([]resourcescore.Run, 0), nil
	}

	runs := make([]resourcescore.Run, 0, len(taskARNs))
	for i := 0; i < len(taskARNs); i += ecsDescribeTasksBatchSize {
		end := i + ecsDescribeTasksBatchSize
		if end > len(taskARNs) {
			end = len(taskARNs)
		}
		desc, err := c.ecsSvc.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(clusterARN), Tasks: taskARNs[i:end]})
		if err != nil {
			return nil, fmt.Errorf("describe ecs tasks for cluster %s: %w", clusterARN, err)
		}
		for taskIndex := range desc.Tasks {
			task := desc.Tasks[taskIndex]
			if taskDefinitionARN != "" && aws.ToString(task.TaskDefinitionArn) != taskDefinitionARN {
				continue
			}
			if startedBy != "" && aws.ToString(task.StartedBy) != startedBy {
				continue
			}
			if task.StartedAt == nil || task.StartedAt.Before(since) || (!until.IsZero() && task.StartedAt.After(until)) {
				continue
			}
			run := resourcescore.Run{RunID: helpers.ResourceNameFromARN(aws.ToString(task.TaskArn)), Status: aws.ToString(task.LastStatus), StartAt: helpers.FormatRFC3339UTC(*task.StartedAt), SourceService: "ecs"}
			if task.StoppedAt != nil && !task.StoppedAt.IsZero() {
				run.EndAt = helpers.FormatRFC3339UTC(*task.StoppedAt)
				duration := int64(task.StoppedAt.Sub(*task.StartedAt).Seconds())
				run.DurationSec = &duration
			}
			runs = append(runs, run)
			if len(runs) >= maxResults {
				return runs, nil
			}
		}
	}
	return runs, nil
}

func (c *ecsCollector) collectCloudTrailRuns(ctx context.Context, since time.Time) ([]ecsCloudTrailRun, error) {
	events, err := lookupCloudTrailEvents(ctx, c.ctSvc, ecsCloudTrailEventName, since, time.Time{}, c.caches)
	if err != nil {
		return nil, fmt.Errorf("lookup ecs cloudtrail events: %w", err)
	}
	runs := make([]ecsCloudTrailRun, 0, len(events))
	for eventIndex := range events {
		runs = append(runs, ecsCloudTrailRunsFromEvent(&events[eventIndex], since)...)
	}
	slices.SortStableFunc(runs, func(left, right ecsCloudTrailRun) int {
		return ecsRunSortTime(&right.run).Compare(ecsRunSortTime(&left.run))
	})
	return runs, nil
}

func ecsCloudTrailRequired(since, now time.Time) bool {
	windowEnd := since.Add(ecsTimelineWindowDuration)
	return windowEnd.Before(now.Add(-ecsStoppedTaskRetentionApprox))
}

func filterECSCloudTrailRuns(allRuns []ecsCloudTrailRun, clusterARN string, hints *TargetHints, maxResults int) []resourcescore.Run {
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
