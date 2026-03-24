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

type redshiftCollector struct {
	caches *runCollectorCaches
	ctSvc  *cloudtrail.Client
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
	runs, err := collectCloudTrailRunsForResources(ctx, c.ctSvc, targetAction, clusterIDs, since, until, maxResults, c.caches, c.runsFromEvent)
	if err != nil {
		return nil, fmt.Errorf("collect redshift cloudtrail runs: %w", err)
	}
	return runs, nil
}

func (*redshiftCollector) runsFromEvent(event *cloudtrailtypes.Event, since time.Time) []cloudTrailActionRun {
	return genericCloudTrailRunsFromEvent(
		event,
		since,
		redshiftCloudTrailRequestResourceKeys,
	)
}
