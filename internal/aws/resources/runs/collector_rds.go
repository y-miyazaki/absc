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

type rdsCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
}

type rdsCloudTrailEventEnvelope struct {
	RequestParameters map[string]any `json:"requestParameters"`
	ResponseElements  map[string]any `json:"responseElements"`
	//nolint:tagliatelle // CloudTrail event payload uses eventID.
	EventID string `json:"eventID"`
}

func newRDSCollector(ctSvc *cloudtrail.Client, caches *runCollectorCaches) *rdsCollector {
	return &rdsCollector{caches: caches, ctSvc: ctSvc}
}

func (*rdsCollector) Service() string { return "rds" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *rdsCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = targetARN
	_ = runJobName
	cacheKey := strings.Join(append([]string{schedule.TargetAction}, hints.RDSResourceIDs...), cacheKeySeparator)
	description := fmt.Sprintf("RDS action=%s resources=%d", schedule.TargetAction, len(hints.RDSResourceIDs))
	runs, err := getCachedRunsForCollector(c.caches, c, cacheKey, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, schedule.TargetAction, hints.RDSResourceIDs, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect rds runs for action %s: %w", schedule.TargetAction, err)
	}
	return runs, nil
}
func (c *rdsCollector) collectRuns(ctx context.Context, targetAction string, resourceIDs []string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	runs, err := collectCloudTrailFilteredRuns(ctx, c.ctSvc, targetAction, resourceIDs, since, until, maxResults, c.caches, c.runsFromEvent, c.Service())
	if err != nil {
		return nil, fmt.Errorf("collect rds cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (c *rdsCollector) runsFromEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	if event.CloudTrailEvent == nil || event.EventTime == nil || event.EventTime.Before(since) {
		return nil
	}

	var envelope rdsCloudTrailEventEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(event.CloudTrailEvent)), &envelope); err != nil {
		return nil
	}

	resourceIDs := cloudTrailResourceIDsFromMap(envelope.RequestParameters, []string{"dBClusterIdentifier", "dbClusterIdentifier", "DbClusterIdentifier", "dBInstanceIdentifier", "dbInstanceIdentifier", "DbInstanceIdentifier"})
	if len(resourceIDs) == 0 {
		resourceIDs = append(resourceIDs, cloudTrailResourceNames(event, "")...)
	}
	if len(resourceIDs) == 0 {
		return nil
	}

	status := c.runStatus(aws.ToString(event.EventName), c.responseState(&envelope))
	return []cloudTrailActionRun{{
		resourceIDs: resourceIDs,
		run:         cloudTrailRunFromEvent(event, envelope.EventID, status),
	}}
}

func (*rdsCollector) responseState(envelope *rdsCloudTrailEventEnvelope) string {
	if envelope == nil {
		return ""
	}
	return cloudTrailResponseStateFromMap(envelope.ResponseElements, []string{"status", "dBClusterStatus", "dbClusterStatus", "dBInstanceStatus", "dbInstanceStatus"})
}

func (*rdsCollector) runStatus(eventName, responseState string) string {
	event := strings.TrimSpace(eventName)
	_ = responseState

	switch event {
	case "StartDBCluster", "StartDBInstance":
		return "START_REQUESTED"
	case "StopDBCluster", "StopDBInstance":
		return "STOP_REQUESTED"
	case "RebootDBInstance":
		return "REBOOT_REQUESTED"
	default:
		return "ACTION_REQUESTED"
	}
}
