// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: shared collector helpers are intentionally compact.
package runs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

type runCollector interface {
	//nolint:inamedparam // Interface readability is secondary here; implementations carry the parameter names.
	Collect(context.Context, *resourcescore.Schedule, string, string, TargetHints, resourcescore.CollectOptions) ([]resourcescore.Run, error)
	Service() string
}

type runCollectorCaches struct {
	cloudTrailEventErrCache map[string]error
	cloudTrailEventsCache   map[string][]cloudtrailtypes.Event
	runErrCache             map[string]map[string]error
	runResultsCache         map[string]map[string][]resourcescore.Run
}

func newRunCollectorCaches() *runCollectorCaches {
	return &runCollectorCaches{
		cloudTrailEventErrCache: make(map[string]error),
		cloudTrailEventsCache:   make(map[string][]cloudtrailtypes.Event),
		runErrCache:             make(map[string]map[string]error),
		runResultsCache:         make(map[string]map[string][]resourcescore.Run),
	}
}

func ensureServiceRunCaches(caches *runCollectorCaches, service string) (map[string][]resourcescore.Run, map[string]error) {
	runsCache, ok := caches.runResultsCache[service]
	if !ok {
		runsCache = make(map[string][]resourcescore.Run)
		caches.runResultsCache[service] = runsCache
	}
	errCache, ok := caches.runErrCache[service]
	if !ok {
		errCache = make(map[string]error)
		caches.runErrCache[service] = errCache
	}
	return runsCache, errCache
}

func getCachedRunsForCollector(caches *runCollectorCaches, collector runCollector, key, description string, collectFn func() ([]resourcescore.Run, error)) ([]resourcescore.Run, error) {
	runsCache, errCache := ensureServiceRunCaches(caches, collector.Service())
	runs, err := getCachedRuns(runsCache, errCache, key, description, collectFn)
	if err != nil {
		return nil, fmt.Errorf("collect cached runs for service %s: %w", collector.Service(), err)
	}
	return runs, nil
}

func collectCloudTrailFilteredRuns(
	ctx context.Context,
	svc *cloudtrail.Client,
	targetAction string,
	resourceIDs []string,
	since, until time.Time,
	maxResults int,
	caches *runCollectorCaches,
	parser func(*cloudtrailtypes.Event, time.Time) []cloudTrailActionRun,
	serviceName string,
) ([]resourcescore.Run, error) {
	allRuns, err := collectCloudTrailActionRuns(ctx, svc, targetAction, since, until, caches, parser)
	if err != nil {
		return nil, fmt.Errorf("collect %s cloudtrail runs: %w", serviceName, err)
	}
	return filterCloudTrailActionRuns(allRuns, resourceIDs, maxResults), nil
}

// getCachedRuns retrieves cached runs or executes collectFn to populate the cache.
// The description parameter is used for error messages instead of the cache key,
// providing clearer diagnostics when cache operations fail.
func getCachedRuns(runsCache map[string][]resourcescore.Run, errCache map[string]error, key, description string, collectFn func() ([]resourcescore.Run, error)) ([]resourcescore.Run, error) {
	if runs, ok := runsCache[key]; ok {
		return runs, nil
	}
	if err, ok := errCache[key]; ok {
		return nil, err
	}

	runs, err := collectFn()
	if err != nil {
		errCache[key] = err
		return nil, fmt.Errorf("collect runs for %s: %w", description, err)
	}
	runsCache[key] = runs
	return runs, nil
}
