// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: CloudTrail parser code is intentionally compact.
package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

type redshiftCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
}

type redshiftCloudTrailEventEnvelope struct {
	RequestParameters map[string]any `json:"requestParameters"`
	ResponseElements  map[string]any `json:"responseElements"`
	//nolint:tagliatelle // CloudTrail event payload uses eventID.
	EventID string `json:"eventID"`
}

func newRedshiftCollector(ctSvc *cloudtrail.Client, caches *runCollectorCaches) *redshiftCollector {
	return &redshiftCollector{caches: caches, ctSvc: ctSvc}
}

func (*redshiftCollector) Service() string { return "redshift" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *redshiftCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = targetARN
	_ = runJobName
	cacheKey := strings.Join(append([]string{schedule.TargetAction}, hints.RedshiftClusterIDs...), cacheKeySeparator)
	description := fmt.Sprintf("Redshift action=%s clusters=%d", schedule.TargetAction, len(hints.RedshiftClusterIDs))
	runs, err := getCachedRunsForCollector(c.caches, c, cacheKey, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, schedule.TargetAction, hints.RedshiftClusterIDs, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect redshift runs for action %s: %w", schedule.TargetAction, err)
	}
	return runs, nil
}

func (c *redshiftCollector) collectRuns(ctx context.Context, targetAction string, clusterIDs []string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	runs, err := collectCloudTrailFilteredRuns(ctx, c.ctSvc, targetAction, clusterIDs, since, until, maxResults, c.caches, c.runsFromEvent, c.Service())
	if err != nil {
		return nil, fmt.Errorf("collect redshift cloudtrail runs: %w", err)
	}
	return runs, nil
}

//nolint:revive // receiver is required by interface, see RDS collector pattern.
func (c *redshiftCollector) runsFromEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	if event.CloudTrailEvent == nil || event.EventTime == nil || event.EventTime.Before(since) {
		return nil
	}

	var envelope redshiftCloudTrailEventEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(event.CloudTrailEvent)), &envelope); err != nil {
		return nil
	}

	clusterIDs := cloudTrailResourceIDsFromMap(envelope.RequestParameters, []string{"clusterIdentifier", "ClusterIdentifier"})
	// Fallback to response elements
	if len(clusterIDs) == 0 {
		clusterIDs = append(clusterIDs, cloudTrailResourceNames(event, "")...)
	}
	if len(clusterIDs) == 0 {
		return nil
	}

	status := c.runStatus(aws.ToString(event.EventName), c.responseState(&envelope))
	return []cloudTrailActionRun{{
		resourceIDs: clusterIDs,
		run:         cloudTrailRunFromEvent(event, envelope.EventID, status),
	}}
}

func (*redshiftCollector) responseState(envelope *redshiftCloudTrailEventEnvelope) string {
	if envelope == nil {
		return ""
	}
	return cloudTrailResponseStateFromMap(envelope.ResponseElements, []string{"status", "clusterStatus", "ClusterStatus"})
}

func (*redshiftCollector) runStatus(eventName, responseState string) string {
	event := strings.TrimSpace(eventName)
	_ = responseState

	switch event {
	case "PauseCluster":
		return "STOP_REQUESTED"
	case "ResumeCluster":
		return "START_REQUESTED"
	case "RebootCluster":
		return "REBOOT_REQUESTED"
	case "CreateCluster":
		return "CREATE_REQUESTED"
	case "DeleteCluster":
		return "DELETE_REQUESTED"
	default:
		return "ACTION_REQUESTED"
	}
}
