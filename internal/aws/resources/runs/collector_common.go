// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: shared collector helpers are intentionally compact.
package runs

import (
	"context"
	"fmt"

	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

type runCollector interface {
	//nolint:inamedparam // Interface readability is secondary here; implementations carry the parameter names.
	Collect(context.Context, *resourcescore.Schedule, string, string, TargetHints, resourcescore.CollectOptions) ([]resourcescore.Run, error)
	Service() string
}

type runCollectorCaches struct {
	batchErrCache           map[string]error
	batchRunsCache          map[string][]resourcescore.Run
	cloudTrailEventErrCache map[string]error
	cloudTrailEventsCache   map[string][]cloudtrailtypes.Event
	ec2ErrCache             map[string]error
	ec2RunsCache            map[string][]resourcescore.Run
	ecsErrCache             map[string]error
	ecsRunsCache            map[string][]resourcescore.Run
	glueErrCache            map[string]error
	glueRunsCache           map[string][]resourcescore.Run
	lambdaErrCache          map[string]error
	lambdaRunsCache         map[string][]resourcescore.Run
	rdsErrCache             map[string]error
	rdsRunsCache            map[string][]resourcescore.Run
	stepErrCache            map[string]error
	stepRunsCache           map[string][]resourcescore.Run
}

func newRunCollectorCaches() *runCollectorCaches {
	return &runCollectorCaches{
		batchErrCache:           make(map[string]error),
		batchRunsCache:          make(map[string][]resourcescore.Run),
		cloudTrailEventErrCache: make(map[string]error),
		cloudTrailEventsCache:   make(map[string][]cloudtrailtypes.Event),
		ec2ErrCache:             make(map[string]error),
		ec2RunsCache:            make(map[string][]resourcescore.Run),
		ecsErrCache:             make(map[string]error),
		ecsRunsCache:            make(map[string][]resourcescore.Run),
		glueErrCache:            make(map[string]error),
		glueRunsCache:           make(map[string][]resourcescore.Run),
		lambdaErrCache:          make(map[string]error),
		lambdaRunsCache:         make(map[string][]resourcescore.Run),
		rdsErrCache:             make(map[string]error),
		rdsRunsCache:            make(map[string][]resourcescore.Run),
		stepErrCache:            make(map[string]error),
		stepRunsCache:           make(map[string][]resourcescore.Run),
	}
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
