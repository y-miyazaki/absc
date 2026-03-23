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

var runCollectorRegistrations = []struct {
	build      func(runCollectorDeps) runCollector
	targetKind string
}{
	{
		targetKind: "batch",
		build: func(deps runCollectorDeps) runCollector {
			return newBatchCollector(deps.batchSvc, deps.caches)
		},
	},
	{
		targetKind: "ec2",
		build: func(deps runCollectorDeps) runCollector {
			return newEC2Collector(deps.ctSvc, deps.caches)
		},
	},
	{
		targetKind: "ecs",
		build: func(deps runCollectorDeps) runCollector {
			return newECSCollector(deps.ecsSvc, deps.ctSvc, deps.caches)
		},
	},
	{
		targetKind: "glue",
		build: func(deps runCollectorDeps) runCollector {
			return newGlueCollector(deps.glueSvc, deps.caches)
		},
	},
	{
		targetKind: "lambda",
		build: func(deps runCollectorDeps) runCollector {
			return newLambdaCollector(deps.cwlSvc, deps.caches)
		},
	},
	{
		targetKind: "rds",
		build: func(deps runCollectorDeps) runCollector {
			return newRDSCollector(deps.ctSvc, deps.caches)
		},
	},
	{
		targetKind: "stepfunctions",
		build: func(deps runCollectorDeps) runCollector {
			return newStepFunctionsCollector(deps.stepSvc, deps.caches)
		},
	},
}

var supportedRunTargetKinds = func() []string {
	targetKinds := make([]string, 0, len(runCollectorRegistrations))
	for _, registration := range runCollectorRegistrations {
		targetKinds = append(targetKinds, registration.targetKind)
	}
	return targetKinds
}()

type TargetHints struct {
	EC2InstanceIDs       []string
	ECSRoleARN           string
	ECSStartedBy         string
	ECSTaskDefinitionARN string
	RDSResourceIDs       []string
}

type runCollectorDeps struct {
	batchSvc *batch.Client
	caches   *runCollectorCaches
	ctSvc    *cloudtrail.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	stepSvc  *sfn.Client
}

type Resolver struct {
	collectors map[string]runCollector
	region     string
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

func newCollectors(deps runCollectorDeps) map[string]runCollector {
	collectors := make(map[string]runCollector, len(supportedRunTargetKinds))
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
