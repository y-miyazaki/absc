//revive:disable:comments-density reason: dispatcher file is intentionally compact and repetitive.
package resources

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

const cacheKeySeparator = "|"

type runCollectorEntry struct {
	collect runCollectorFunc
	service string
}

type runCollectorFunc func(context.Context, string, string, runTargetHints, CollectOptions, runCollectorDeps, *runCollectorCaches) ([]Run, error)

// runCollectorDeps groups shared AWS service clients used while resolving runs.
type runCollectorDeps struct {
	batchSvc *batch.Client
	ctSvc    *cloudtrail.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	stepSvc  *sfn.Client
	region   string
}

func newRunCollectorDeps(region string, stepSvc *sfn.Client, batchSvc *batch.Client, ctSvc *cloudtrail.Client, ecsSvc *ecs.Client, glueSvc *glue.Client, cwlSvc *cloudwatchlogs.Client) runCollectorDeps {
	return runCollectorDeps{
		region:   region,
		stepSvc:  stepSvc,
		batchSvc: batchSvc,
		ctSvc:    ctSvc,
		ecsSvc:   ecsSvc,
		glueSvc:  glueSvc,
		cwlSvc:   cwlSvc,
	}
}

// runTargetHints carries optional target-specific filters for run lookups.
type runTargetHints struct {
	ecsRoleARN           string
	ecsStartedBy         string
	ecsTaskDefinitionARN string
}

// runCollectorCaches stores per-target run lookups to avoid duplicate API calls.
// Successes and failures are both memoized for one collection pass.
type runCollectorCaches struct {
	ecsCTErr        error
	lambdaErrCache  map[string]error
	batchRunsCache  map[string][]Run
	ecsErrCache     map[string]error
	ecsRunsCache    map[string][]Run
	glueErrCache    map[string]error
	glueRunsCache   map[string][]Run
	batchErrCache   map[string]error
	lambdaRunsCache map[string][]Run
	stepErrCache    map[string]error
	stepRunsCache   map[string][]Run
	ecsCTRuns       []ecsCloudTrailRun
	ecsCTLoaded     bool
}

var runCollectorsByKind = map[string]runCollectorEntry{
	"batch": {
		service: "batch",
		collect: collectBatchRunsForTarget,
	},
	"ecs": {
		service: "ecs",
		collect: collectECSRunsForTarget,
	},
	"glue": {
		service: "glue",
		collect: collectGlueRunsForTarget,
	},
	"lambda": {
		service: "lambda",
		collect: collectLambdaRunsForTarget,
	},
	"stepfunctions": {
		service: "stepfunctions",
		collect: collectStepFunctionRunsForTarget,
	},
}

// newRunCollectorCaches allocates all caches used during a collection cycle.
func newRunCollectorCaches() *runCollectorCaches {
	return &runCollectorCaches{
		stepRunsCache:   make(map[string][]Run),
		stepErrCache:    make(map[string]error),
		batchRunsCache:  make(map[string][]Run),
		batchErrCache:   make(map[string]error),
		ecsCTRuns:       make([]ecsCloudTrailRun, 0),
		ecsRunsCache:    make(map[string][]Run),
		ecsErrCache:     make(map[string]error),
		glueRunsCache:   make(map[string][]Run),
		glueErrCache:    make(map[string]error),
		lambdaRunsCache: make(map[string][]Run),
		lambdaErrCache:  make(map[string]error),
	}
}

// collectRunsByTargetKind routes each target to the service-specific run collector.
// Cache lookups keep repeated targets within the same export from re-querying AWS.
func collectRunsByTargetKind(ctx context.Context, targetKind, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, *ErrorRecord) {
	entry, ok := runCollectorsByKind[targetKind]
	if !ok {
		return nil, nil
	}

	runs, err := entry.collect(ctx, targetARN, jobName, hints, opts, deps, caches)
	if err != nil {
		return nil, &ErrorRecord{Service: entry.service, Region: deps.region, Message: err.Error()}
	}
	return runs, nil
}

func collectBatchRunsForTarget(ctx context.Context, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, error) {
	_ = hints
	cacheKey := targetARN + cacheKeySeparator + jobName
	runs, err := getCachedRuns(caches.batchRunsCache, caches.batchErrCache, cacheKey, func() ([]Run, error) {
		return collectBatchRuns(ctx, deps.batchSvc, targetARN, jobName, opts.Since, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect batch runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func collectECSRunsForTarget(ctx context.Context, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, error) {
	_ = jobName
	cacheKey := targetARN + cacheKeySeparator + hints.ecsTaskDefinitionARN + cacheKeySeparator + hints.ecsStartedBy + cacheKeySeparator + hints.ecsRoleARN
	runs, err := getCachedRuns(caches.ecsRunsCache, caches.ecsErrCache, cacheKey, func() ([]Run, error) {
		return collectECSRuns(ctx, deps.ecsSvc, deps.ctSvc, targetARN, hints, opts.Since, opts.MaxResults, caches)
	})
	if err != nil {
		return nil, fmt.Errorf("collect ecs runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func collectGlueRunsForTarget(ctx context.Context, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.glueRunsCache, caches.glueErrCache, targetARN, func() ([]Run, error) {
		return collectGlueRuns(ctx, deps.glueSvc, targetARN, opts.Since, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect glue runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func collectLambdaRunsForTarget(ctx context.Context, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.lambdaRunsCache, caches.lambdaErrCache, targetARN, func() ([]Run, error) {
		return collectLambdaRuns(ctx, deps.cwlSvc, targetARN, opts.Since, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect lambda runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func collectStepFunctionRunsForTarget(ctx context.Context, targetARN, jobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.stepRunsCache, caches.stepErrCache, targetARN, func() ([]Run, error) {
		return collectStepFunctionRuns(ctx, deps.stepSvc, targetARN, opts.Since, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect step function runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

func populateScheduleRuns(ctx context.Context, schedule *Schedule, runTargetARN, runJobName string, hints runTargetHints, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) *ErrorRecord {
	runs, runErr := collectRunsByTargetKind(ctx, schedule.TargetKind, runTargetARN, runJobName, hints, opts, deps, caches)
	if runErr != nil {
		return runErr
	}
	if runs == nil {
		return nil
	}
	schedule.Runs = runs
	if opts.MaxResults > 0 && len(runs) >= opts.MaxResults {
		schedule.RunsCapped = true
	}
	return nil
}

// getCachedRuns stores both successful and failed lookups for the current run.
func getCachedRuns(runsCache map[string][]Run, errCache map[string]error, key string, collectFn func() ([]Run, error)) ([]Run, error) {
	if runs, ok := runsCache[key]; ok {
		return runs, nil
	}
	if err, ok := errCache[key]; ok {
		return nil, err
	}

	runs, err := collectFn()
	if err != nil {
		errCache[key] = err
		return nil, fmt.Errorf("collect cached runs for %s: %w", key, err)
	}
	runsCache[key] = runs
	return runs, nil
}
