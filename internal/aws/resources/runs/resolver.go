// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: dispatcher-style collector routing is intentionally compact.
package runs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

const (
	cacheKeySeparator = "|"
	defaultMaxResults = 144
)

type TargetHints struct {
	ECSRoleARN           string
	ECSStartedBy         string
	ECSTaskDefinitionARN string
}

type runCollectorEntry struct {
	collect runCollectorFunc
	service string
}

type runCollectorFunc func(context.Context, string, string, TargetHints, resourcescore.CollectOptions, runCollectorDeps, *runCollectorCaches) ([]resourcescore.Run, error)

type runCollectorDeps struct {
	batchSvc *batch.Client
	ctSvc    *cloudtrail.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	stepSvc  *sfn.Client
	region   string
}

type runCollectorCaches struct {
	ecsCTErr        error
	lambdaErrCache  map[string]error
	batchRunsCache  map[string][]resourcescore.Run
	ecsErrCache     map[string]error
	ecsRunsCache    map[string][]resourcescore.Run
	glueErrCache    map[string]error
	glueRunsCache   map[string][]resourcescore.Run
	batchErrCache   map[string]error
	lambdaRunsCache map[string][]resourcescore.Run
	stepErrCache    map[string]error
	stepRunsCache   map[string][]resourcescore.Run
	ecsCTRuns       []ecsCloudTrailRun
	ecsCTLoaded     bool
}

type Resolver struct {
	caches *runCollectorCaches
	deps   runCollectorDeps
}

var runCollectorsByKind = map[string]runCollectorEntry{
	"batch": {
		collect: collectBatchRunsForTarget,
		service: "batch",
	},
	"ecs": {
		collect: collectECSRunsForTarget,
		service: "ecs",
	},
	"glue": {
		collect: collectGlueRunsForTarget,
		service: "glue",
	},
	"lambda": {
		collect: collectLambdaRunsForTarget,
		service: "lambda",
	},
	"stepfunctions": {
		collect: collectStepFunctionRunsForTarget,
		service: "stepfunctions",
	},
}

func NewResolver(region string, stepSvc *sfn.Client, batchSvc *batch.Client, ctSvc *cloudtrail.Client, ecsSvc *ecs.Client, glueSvc *glue.Client, cwlSvc *cloudwatchlogs.Client) *Resolver {
	return &Resolver{
		caches: newRunCollectorCaches(),
		deps: runCollectorDeps{
			batchSvc: batchSvc,
			ctSvc:    ctSvc,
			cwlSvc:   cwlSvc,
			ecsSvc:   ecsSvc,
			glueSvc:  glueSvc,
			region:   region,
			stepSvc:  stepSvc,
		},
	}
}

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (r *Resolver) PopulateScheduleRuns(ctx context.Context, schedule *resourcescore.Schedule, runTargetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) *resourcescore.ErrorRecord {
	runs, runErr := collectRunsByTargetKind(ctx, schedule.TargetKind, runTargetARN, runJobName, hints, opts, r.deps, r.caches)
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

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectRunsByTargetKind(ctx context.Context, targetKind, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, *resourcescore.ErrorRecord) {
	entry, ok := runCollectorsByKind[targetKind]
	if !ok {
		return nil, nil
	}

	runs, err := entry.collect(ctx, targetARN, jobName, hints, opts, deps, caches)
	if err != nil {
		return nil, &resourcescore.ErrorRecord{Service: entry.service, Region: deps.region, Message: err.Error()}
	}
	return runs, nil
}

func newRunCollectorCaches() *runCollectorCaches {
	return &runCollectorCaches{
		batchErrCache:   make(map[string]error),
		batchRunsCache:  make(map[string][]resourcescore.Run),
		ecsCTRuns:       make([]ecsCloudTrailRun, 0),
		ecsErrCache:     make(map[string]error),
		ecsRunsCache:    make(map[string][]resourcescore.Run),
		glueErrCache:    make(map[string]error),
		glueRunsCache:   make(map[string][]resourcescore.Run),
		lambdaErrCache:  make(map[string]error),
		lambdaRunsCache: make(map[string][]resourcescore.Run),
		stepErrCache:    make(map[string]error),
		stepRunsCache:   make(map[string][]resourcescore.Run),
	}
}

func getCachedRuns(runsCache map[string][]resourcescore.Run, errCache map[string]error, key string, collectFn func() ([]resourcescore.Run, error)) ([]resourcescore.Run, error) {
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

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectBatchRunsForTarget(ctx context.Context, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	_ = hints
	cacheKey := targetARN + cacheKeySeparator + jobName
	runs, err := getCachedRuns(caches.batchRunsCache, caches.batchErrCache, cacheKey, func() ([]resourcescore.Run, error) {
		return collectBatchRuns(ctx, deps.batchSvc, targetARN, jobName, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect batch runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectECSRunsForTarget(ctx context.Context, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	_ = jobName
	cacheKey := targetARN + cacheKeySeparator + hints.ECSTaskDefinitionARN + cacheKeySeparator + hints.ECSStartedBy + cacheKeySeparator + hints.ECSRoleARN
	runs, err := getCachedRuns(caches.ecsRunsCache, caches.ecsErrCache, cacheKey, func() ([]resourcescore.Run, error) {
		return collectECSRuns(ctx, deps.ecsSvc, deps.ctSvc, targetARN, hints, opts.Since, opts.Until, opts.MaxResults, caches)
	})
	if err != nil {
		return nil, fmt.Errorf("collect ecs runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectGlueRunsForTarget(ctx context.Context, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.glueRunsCache, caches.glueErrCache, targetARN, func() ([]resourcescore.Run, error) {
		return collectGlueRuns(ctx, deps.glueSvc, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect glue runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectLambdaRunsForTarget(ctx context.Context, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.lambdaRunsCache, caches.lambdaErrCache, targetARN, func() ([]resourcescore.Run, error) {
		return collectLambdaRuns(ctx, deps.cwlSvc, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect lambda runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}

//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func collectStepFunctionRunsForTarget(ctx context.Context, targetARN, jobName string, hints TargetHints, opts resourcescore.CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]resourcescore.Run, error) {
	_ = jobName
	_ = hints
	runs, err := getCachedRuns(caches.stepRunsCache, caches.stepErrCache, targetARN, func() ([]resourcescore.Run, error) {
		return collectStepFunctionRuns(ctx, deps.stepSvc, targetARN, opts.Since, opts.Until, opts.MaxResults)
	})
	if err != nil {
		return nil, fmt.Errorf("collect step function runs for target %s: %w", targetARN, err)
	}
	return runs, nil
}
