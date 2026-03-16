package resources

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

type runCollectorDeps struct {
	batchSvc *batch.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	region   string
	stepSvc  *sfn.Client
}

type runCollectorCaches struct {
	batchErrCache   map[string]error
	batchRunsCache  map[string][]Run
	ecsErrCache     map[string]error
	ecsRunsCache    map[string][]Run
	glueErrCache    map[string]error
	glueRunsCache   map[string][]Run
	lambdaErrCache  map[string]error
	lambdaRunsCache map[string][]Run
	stepErrCache    map[string]error
	stepRunsCache   map[string][]Run
}

func newRunCollectorCaches() *runCollectorCaches {
	return &runCollectorCaches{
		stepRunsCache:   make(map[string][]Run),
		stepErrCache:    make(map[string]error),
		batchRunsCache:  make(map[string][]Run),
		batchErrCache:   make(map[string]error),
		ecsRunsCache:    make(map[string][]Run),
		ecsErrCache:     make(map[string]error),
		glueRunsCache:   make(map[string][]Run),
		glueErrCache:    make(map[string]error),
		lambdaRunsCache: make(map[string][]Run),
		lambdaErrCache:  make(map[string]error),
	}
}

func collectRunsByTargetKind(ctx context.Context, targetKind, targetARN, jobName string, opts CollectOptions, deps runCollectorDeps, caches *runCollectorCaches) ([]Run, *ErrorRecord) {
	switch targetKind {
	case "stepfunctions":
		runs, err := getCachedRuns(caches.stepRunsCache, caches.stepErrCache, targetARN, func() ([]Run, error) {
			return collectStepFunctionRuns(ctx, deps.stepSvc, targetARN, opts.Since, opts.MaxResults)
		})
		if err != nil {
			return nil, &ErrorRecord{Service: "stepfunctions", Region: deps.region, Message: err.Error()}
		}
		return runs, nil
	case "batch":
		cacheKey := targetARN + "|" + jobName
		runs, err := getCachedRuns(caches.batchRunsCache, caches.batchErrCache, cacheKey, func() ([]Run, error) {
			return collectBatchRuns(ctx, deps.batchSvc, targetARN, jobName, opts.Since, opts.MaxResults)
		})
		if err != nil {
			return nil, &ErrorRecord{Service: "batch", Region: deps.region, Message: err.Error()}
		}
		return runs, nil
	case "ecs":
		runs, err := getCachedRuns(caches.ecsRunsCache, caches.ecsErrCache, targetARN, func() ([]Run, error) {
			return collectECSRuns(ctx, deps.ecsSvc, targetARN, opts.Since, opts.MaxResults)
		})
		if err != nil {
			return nil, &ErrorRecord{Service: "ecs", Region: deps.region, Message: err.Error()}
		}
		return runs, nil
	case "glue":
		runs, err := getCachedRuns(caches.glueRunsCache, caches.glueErrCache, targetARN, func() ([]Run, error) {
			return collectGlueRuns(ctx, deps.glueSvc, targetARN, opts.Since, opts.MaxResults)
		})
		if err != nil {
			return nil, &ErrorRecord{Service: "glue", Region: deps.region, Message: err.Error()}
		}
		return runs, nil
	case "lambda":
		runs, err := getCachedRuns(caches.lambdaRunsCache, caches.lambdaErrCache, targetARN, func() ([]Run, error) {
			return collectLambdaRuns(ctx, deps.cwlSvc, targetARN, opts.Since, opts.MaxResults)
		})
		if err != nil {
			return nil, &ErrorRecord{Service: "lambda", Region: deps.region, Message: err.Error()}
		}
		return runs, nil
	default:
		return nil, nil
	}
}

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
		return nil, err
	}
	runsCache[key] = runs
	return runs, nil
}
