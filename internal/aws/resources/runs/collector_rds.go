// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: CloudTrail parser code is intentionally compact.
package runs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

type rdsCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
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
	runs, err := collectCloudTrailRunsForResources(ctx, c.ctSvc, targetAction, resourceIDs, since, until, maxResults, c.caches, c.runsFromEvent)
	if err != nil {
		return nil, fmt.Errorf("collect rds cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (*rdsCollector) runsFromEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	return genericCloudTrailRunsFromEvent(
		event,
		since,
		rdsCloudTrailRequestResourceKeys,
	)
}
