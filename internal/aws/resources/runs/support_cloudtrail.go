// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: CloudTrail helpers are intentionally compact.
package runs

import (
	"context"
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
	cloudTrailLookupMaxResults int32 = 50
	cloudTrailSplitParts             = 2
	cloudTrailWindowDuration         = 24 * time.Hour
)

type cloudTrailActionRun struct {
	run         resourcescore.Run
	resourceIDs []string
}

func collectCloudTrailActionRuns(ctx context.Context, svc *cloudtrail.Client, targetAction string, since, until time.Time, caches *runCollectorCaches, parser func(*cloudtrailtypes.Event, time.Time) []cloudTrailActionRun) ([]cloudTrailActionRun, error) {
	eventName := cloudTrailEventName(targetAction)
	if eventName == "" {
		return make([]cloudTrailActionRun, 0), nil
	}
	events, err := lookupCloudTrailEvents(ctx, svc, eventName, since, until, caches)
	if err != nil {
		return nil, fmt.Errorf("lookup cloudtrail events for %s: %w", eventName, err)
	}
	runs := make([]cloudTrailActionRun, 0, len(events))
	for eventIndex := range events {
		runs = append(runs, parser(&events[eventIndex], since)...)
	}
	slices.SortStableFunc(runs, func(left, right cloudTrailActionRun) int {
		return cloudTrailRunSortTime(&right.run).Compare(cloudTrailRunSortTime(&left.run))
	})
	return runs, nil
}

func filterCloudTrailActionRuns(allRuns []cloudTrailActionRun, resourceIDs []string, maxResults int) []resourcescore.Run {
	if len(resourceIDs) == 0 {
		return make([]resourcescore.Run, 0)
	}
	resourceIDSet := make(map[string]struct{}, len(resourceIDs))
	for _, resourceID := range resourceIDs {
		trimmed := strings.TrimSpace(resourceID)
		if trimmed == "" {
			continue
		}
		resourceIDSet[trimmed] = struct{}{}
	}
	if len(resourceIDSet) == 0 {
		return make([]resourcescore.Run, 0)
	}

	matches := make([]resourcescore.Run, 0, maxResults)
	for runIndex := range allRuns {
		candidate := allRuns[runIndex]
		for _, resourceID := range candidate.resourceIDs {
			if _, ok := resourceIDSet[resourceID]; !ok {
				continue
			}
			matches = append(matches, candidate.run)
			break
		}
		if maxResults > 0 && len(matches) >= maxResults {
			return matches
		}
	}
	return matches
}

func lookupCloudTrailEvents(ctx context.Context, svc *cloudtrail.Client, eventName string, since, until time.Time, caches *runCollectorCaches) ([]cloudtrailtypes.Event, error) {
	if svc == nil || strings.TrimSpace(eventName) == "" {
		return nil, nil
	}

	windowEnd := until
	if windowEnd.IsZero() || windowEnd.Before(since) {
		windowEnd = since.Add(cloudTrailWindowDuration)
	}
	cacheKey := strings.Join([]string{eventName, since.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339)}, cacheKeySeparator)
	if events, ok := caches.cloudTrailEventsCache[cacheKey]; ok {
		return events, nil
	}
	if err, ok := caches.cloudTrailEventErrCache[cacheKey]; ok {
		return nil, err
	}

	input := &cloudtrail.LookupEventsInput{
		EndTime:   aws.Time(windowEnd),
		StartTime: aws.Time(since),
		LookupAttributes: []cloudtrailtypes.LookupAttribute{{
			AttributeKey:   cloudtrailtypes.LookupAttributeKeyEventName,
			AttributeValue: aws.String(eventName),
		}},
		MaxResults: aws.Int32(cloudTrailLookupMaxResults),
	}

	events := make([]cloudtrailtypes.Event, 0)
	for {
		page, err := svc.LookupEvents(ctx, input)
		if err != nil {
			wrappedErr := fmt.Errorf("cloudtrail lookup events for %s: %w", eventName, err)
			caches.cloudTrailEventErrCache[cacheKey] = wrappedErr
			return nil, wrappedErr
		}
		events = append(events, page.Events...)
		if page.NextToken == nil || aws.ToString(page.NextToken) == "" {
			break
		}
		input.NextToken = page.NextToken
	}
	slices.SortStableFunc(events, func(left, right cloudtrailtypes.Event) int {
		return aws.ToTime(right.EventTime).Compare(aws.ToTime(left.EventTime))
	})
	caches.cloudTrailEventsCache[cacheKey] = events
	return events, nil
}

func cloudTrailEventName(targetAction string) string {
	action := strings.TrimSpace(targetAction)
	if action == "" {
		return ""
	}
	parts := strings.SplitN(action, ":", cloudTrailSplitParts)
	if len(parts) != 2 || parts[1] == "" {
		return ""
	}
	verb := parts[1]
	return strings.ToUpper(verb[:1]) + verb[1:]
}

func cloudTrailRunSortTime(run *resourcescore.Run) time.Time {
	if parsed, err := time.Parse(time.RFC3339, run.StartAt); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, run.EndAt); err == nil {
		return parsed
	}
	return time.Time{}
}

func cloudTrailResourceNames(event *cloudtrailtypes.Event, prefix string) []string {
	if event == nil {
		return nil
	}
	resourceNames := make([]string, 0, len(event.Resources))
	for _, resource := range event.Resources {
		name := strings.TrimSpace(aws.ToString(resource.ResourceName))
		if name == "" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		resourceNames = append(resourceNames, name)
	}
	return resourceNames
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloudTrailResourceIDsFromMap(values map[string]any, keys []string) []string {
	if len(values) == 0 || len(keys) == 0 {
		return nil
	}
	resourceIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		value, found := values[key]
		if !found {
			continue
		}
		text, textOK := value.(string)
		if !textOK || strings.TrimSpace(text) == "" {
			continue
		}
		resourceIDs = append(resourceIDs, text)
	}
	return resourceIDs
}

func cloudTrailResponseStateFromMap(values map[string]any, keys []string) string {
	resourceIDs := cloudTrailResourceIDsFromMap(values, keys)
	if len(resourceIDs) == 0 {
		return ""
	}
	return resourceIDs[0]
}

func cloudTrailRunFromEvent(event *cloudtrailtypes.Event, envelopeEventID, status string) resourcescore.Run {
	runID := firstNonEmpty(aws.ToString(event.EventId), envelopeEventID, helpers.FormatRFC3339NanoUTC(*event.EventTime))
	return resourcescore.Run{
		RunID:         runID,
		StartAt:       helpers.FormatRFC3339UTC(*event.EventTime),
		SourceService: "cloudtrail",
		Status:        status,
	}
}
