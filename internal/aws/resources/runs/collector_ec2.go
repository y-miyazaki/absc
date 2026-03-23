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

type ec2Collector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
}

type ec2CloudTrailEventEnvelope struct {
	//nolint:tagliatelle // CloudTrail event payload uses eventID.
	EventID string `json:"eventID"`
	//nolint:revive // Nested CloudTrail payload struct is intentional to avoid one-off type noise.
	RequestParameters struct {
		InstancesSet struct {
			Items []struct {
				InstanceID string `json:"instanceId"`
			} `json:"items"`
		} `json:"instancesSet"`
	} `json:"requestParameters"`
}

func newEC2Collector(ctSvc *cloudtrail.Client, caches *runCollectorCaches) *ec2Collector {
	return &ec2Collector{caches: caches, ctSvc: ctSvc}
}

func (*ec2Collector) Service() string { return "ec2" }

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (c *ec2Collector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = targetARN
	_ = runJobName
	cacheKey := strings.Join(append([]string{schedule.TargetAction}, hints.EC2InstanceIDs...), cacheKeySeparator)
	description := fmt.Sprintf("EC2 action=%s instances=%d", schedule.TargetAction, len(hints.EC2InstanceIDs))
	runs, err := getCachedRunsForCollector(c.caches, c, cacheKey, description, func() ([]resourcescore.Run, error) {
		return c.collectRuns(ctx, schedule.TargetAction, hints.EC2InstanceIDs, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect ec2 runs for action %s: %w", schedule.TargetAction, err)
	}
	return runs, nil
}

func (c *ec2Collector) collectRuns(ctx context.Context, targetAction string, instanceIDs []string, since, until time.Time, maxResults int) ([]resourcescore.Run, error) {
	runs, err := collectCloudTrailFilteredRuns(ctx, c.ctSvc, targetAction, instanceIDs, since, until, maxResults, c.caches, c.runsFromEvent, c.Service())
	if err != nil {
		return nil, fmt.Errorf("collect ec2 cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (c *ec2Collector) runsFromEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	if event.CloudTrailEvent == nil || event.EventTime == nil || event.EventTime.Before(since) {
		return nil
	}

	var envelope ec2CloudTrailEventEnvelope
	if err := json.Unmarshal([]byte(aws.ToString(event.CloudTrailEvent)), &envelope); err != nil {
		return nil
	}

	resourceIDs := make([]string, 0, len(envelope.RequestParameters.InstancesSet.Items))
	for _, item := range envelope.RequestParameters.InstancesSet.Items {
		if strings.TrimSpace(item.InstanceID) == "" {
			continue
		}
		resourceIDs = append(resourceIDs, item.InstanceID)
	}
	if len(resourceIDs) == 0 {
		resourceIDs = append(resourceIDs, cloudTrailResourceNames(event, "i-")...)
	}
	if len(resourceIDs) == 0 {
		return nil
	}

	status := c.runStatus(aws.ToString(event.EventName))
	return []cloudTrailActionRun{{
		resourceIDs: resourceIDs,
		run:         cloudTrailRunFromEvent(event, envelope.EventID, status),
	}}
}

func (*ec2Collector) runStatus(eventName string) string {
	switch strings.TrimSpace(eventName) {
	case "StartInstances":
		return "START_REQUESTED"
	case "StopInstances":
		return "STOP_REQUESTED"
	case "RebootInstances":
		return "REBOOT_REQUESTED"
	default:
		return "ACTION_REQUESTED"
	}
}
