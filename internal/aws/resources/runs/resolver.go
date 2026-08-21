// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: dispatcher-style collector routing is intentionally compact.
package runs

import (
	"context"

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
)

var (
	// runCollectorRegistrations defines the available run collectors and their associated target kinds.
	runCollectorRegistrations = []struct {
		build      func(runCollectorDeps) RunCollector
		targetKind string
	}{
		{
			targetKind: "batch",
			build: func(deps runCollectorDeps) RunCollector {
				return newBatchCollector(deps.batchSvc, deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "ec2",
			build: func(deps runCollectorDeps) RunCollector {
				return newEC2Collector(deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "ecs",
			build: func(deps runCollectorDeps) RunCollector {
				return newECSCollector(deps.ecsSvc, deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "glue",
			build: func(deps runCollectorDeps) RunCollector {
				return newGlueCollector(deps.glueSvc, deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "lambda",
			build: func(deps runCollectorDeps) RunCollector {
				return newLambdaCollector(deps.cwlSvc, deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "rds",
			build: func(deps runCollectorDeps) RunCollector {
				return newRDSCollector(deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "stepfunctions",
			build: func(deps runCollectorDeps) RunCollector {
				return newStepFunctionsCollector(deps.stepSvc, deps.ctSvc, deps.caches)
			},
		},
		{
			targetKind: "redshift",
			build: func(deps runCollectorDeps) RunCollector {
				return newRedshiftCollector(deps.ctSvc, deps.caches)
			},
		},
	}
	// supportedRunTargetKinds is a list of target kinds for which run collectors are registered.
	supportedRunTargetKinds = func() []string {
		targetKinds := make([]string, 0, len(runCollectorRegistrations))
		for _, registration := range runCollectorRegistrations {
			targetKinds = append(targetKinds, registration.targetKind)
		}
		return targetKinds
	}()
)

// Resolver dispatches execution-history lookups to target-specific collectors.
type Resolver struct {
	collectors map[string]RunCollector
	region     string
}

// TargetHints aliases the shared core hint type for run collectors.
type TargetHints = resourcescore.TargetHints

type runCollectorDeps struct {
	batchSvc *batch.Client
	caches   *runCollectorCaches
	ctSvc    *cloudtrail.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	stepSvc  *sfn.Client
}

// NewResolver creates a new Resolver that fetches execution history for schedule targets.
// Supported target kinds are listed in supportedRunTargetKinds.
// Each collector uses AWS service-specific clients to retrieve run data.
func NewResolver(region string, stepSvc *sfn.Client, batchSvc *batch.Client, ctSvc *cloudtrail.Client, ecsSvc *ecs.Client, glueSvc *glue.Client, cwlSvc *cloudwatchlogs.Client) *Resolver {
	caches := newRunCollectorCaches()
	deps := runCollectorDeps{
		batchSvc: batchSvc,
		caches:   caches,
		ctSvc:    ctSvc,
		cwlSvc:   cwlSvc,
		ecsSvc:   ecsSvc,
		glueSvc:  glueSvc,
		stepSvc:  stepSvc,
	}
	return &Resolver{
		collectors: newCollectors(deps),
		region:     region,
	}
}

func newCollectors(deps runCollectorDeps) map[string]RunCollector {
	collectors := make(map[string]RunCollector, len(supportedRunTargetKinds))
	for _, registration := range runCollectorRegistrations {
		collectors[registration.targetKind] = registration.build(deps)
	}
	return collectors
}

// PopulateScheduleRuns fetches execution history for the given schedule and populates the Runs field.
// It returns an ErrorRecord if collection fails for an observable target kind, or nil if successful
// or the target kind is not supported by any collector.
// The schedule's RunsCapped field is set to true if the returned runs count equals or exceeds MaxResults.
//
//nolint:gocritic // CollectOptions is shared as a value object across collectors.
func (r *Resolver) PopulateScheduleRuns(ctx context.Context, schedule *resourcescore.Schedule, runTargetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) *resourcescore.ErrorRecord {
	collector, ok := r.collectors[schedule.TargetKind]
	if !ok {
		return nil
	}

	runs, err := collector.Collect(ctx, schedule, runTargetARN, runJobName, hints, opts)
	if err != nil {
		return &resourcescore.ErrorRecord{Service: collector.Service(), Region: r.region, Message: err.Error()}
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
