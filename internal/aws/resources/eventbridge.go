//revive:disable:comments-density reason: collector flow is repetitive and extra comments would add noise.
package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

type EventBridgeCollector struct {
	batchSvc *batch.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	svc      *eventbridge.Client
	stepSvc  *sfn.Client
	region   string
}

// NewEventBridgeCollector builds regional service clients for EventBridge rules.
func NewEventBridgeCollector(cfg *aws.Config, region string) (*EventBridgeCollector, error) {
	return &EventBridgeCollector{
		region:   region,
		svc:      eventbridge.NewFromConfig(*cfg, func(o *eventbridge.Options) { o.Region = region }),
		stepSvc:  sfn.NewFromConfig(*cfg, func(o *sfn.Options) { o.Region = region }),
		batchSvc: batch.NewFromConfig(*cfg, func(o *batch.Options) { o.Region = region }),
		cwlSvc:   cloudwatchlogs.NewFromConfig(*cfg, func(o *cloudwatchlogs.Options) { o.Region = region }),
		ecsSvc:   ecs.NewFromConfig(*cfg, func(o *ecs.Options) { o.Region = region }),
		glueSvc:  glue.NewFromConfig(*cfg, func(o *glue.Options) { o.Region = region }),
	}, nil
}

// Name returns the collector identifier.
func (*EventBridgeCollector) Name() string {
	return "eventbridge_rule"
}

// Collect loads scheduled rules and resolves supported target runs.
func (c *EventBridgeCollector) Collect(ctx context.Context, opts CollectOptions) ([]Schedule, []ErrorRecord) {
	schedules := make([]Schedule, 0)
	errs := make([]ErrorRecord, 0)
	deps := runCollectorDeps{
		region:   c.region,
		stepSvc:  c.stepSvc,
		batchSvc: c.batchSvc,
		ecsSvc:   c.ecsSvc,
		glueSvc:  c.glueSvc,
		cwlSvc:   c.cwlSvc,
	}
	caches := newRunCollectorCaches()

	var nextToken *string
	for {
		page, err := c.svc.ListRules(ctx, &eventbridge.ListRulesInput{NextToken: nextToken})
		if err != nil {
			errs = append(errs, ErrorRecord{Service: "eventbridge_rule", Region: c.region, Message: err.Error()})
			return schedules, errs
		}
		for ruleIndex := range page.Rules {
			r := page.Rules[ruleIndex]
			expr := aws.ToString(r.ScheduleExpression)
			if expr == "" {
				continue
			}
			enabled := strings.EqualFold(string(r.State), "ENABLED")
			allTargets := make([]eventbridgetypes.Target, 0)
			var targetsToken *string
			for {
				targets, listTargetsErr := c.svc.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{Rule: r.Name, NextToken: targetsToken})
				if listTargetsErr != nil {
					errs = append(errs, ErrorRecord{Service: "eventbridge_rule", Region: c.region, Message: listTargetsErr.Error()})
					allTargets = nil
					break
				}
				allTargets = append(allTargets, targets.Targets...)
				if targets.NextToken == nil {
					break
				}
				targetsToken = targets.NextToken
			}
			if allTargets == nil {
				continue
			}
			if len(allTargets) == 0 {
				// EventBridge Rule APIs do not expose a deterministic "next invocation" timestamp,
				// so this collector intentionally leaves NextInvocationAt empty.
				schedules = append(schedules, Schedule{
					ID:                 fmt.Sprintf("eventbridge_rule:%s:%s:no-target", c.region, aws.ToString(r.Name)),
					Service:            "eventbridge_rule",
					ScheduleName:       aws.ToString(r.Name),
					ScheduleExpression: expr,
					Enabled:            enabled,
					Region:             c.region,
					TargetKind:         "other",
					TargetService:      "Other",
					NextInvocationAt:   "",
					Slots:              buildSlots(expr),
					Runs:               make([]Run, 0),
				})
				continue
			}

			for i := range allTargets {
				t := allTargets[i]
				targetARN := aws.ToString(t.Arn)
				targetKind := detectTargetKind(targetARN, t.BatchParameters != nil)
				targetService := detectTargetService(targetARN)
				hints := runTargetHints{}
				if t.EcsParameters != nil {
					hints.ecsTaskDefinitionARN = aws.ToString(t.EcsParameters.TaskDefinitionArn)
				}
				// EventBridge Rule APIs do not expose a deterministic "next invocation" timestamp,
				// so this collector intentionally leaves NextInvocationAt empty.
				s := Schedule{
					ID:                 fmt.Sprintf("eventbridge_rule:%s:%s:%d", c.region, aws.ToString(r.Name), i),
					Service:            "eventbridge_rule",
					ScheduleName:       aws.ToString(r.Name),
					ScheduleExpression: expr,
					Enabled:            enabled,
					Region:             c.region,
					TargetARN:          targetARN,
					TargetKind:         targetKind,
					TargetAction:       detectTargetAction(targetARN),
					TargetService:      targetService,
					TargetName:         resourceNameFromARN(targetARN),
					NextInvocationAt:   "",
					Slots:              buildSlots(expr),
					Runs:               make([]Run, 0),
				}

				jobName := ""
				if t.BatchParameters != nil {
					jobName = aws.ToString(t.BatchParameters.JobName)
				}

				runs, runErr := collectRunsByTargetKind(ctx, targetKind, targetARN, jobName, hints, opts, deps, caches)
				if runErr != nil {
					errs = append(errs, *runErr)
				} else if runs != nil {
					s.Runs = runs
					if opts.MaxResults > 0 && len(runs) >= opts.MaxResults {
						s.RunsCapped = true
					}
				}

				schedules = append(schedules, s)
			}
		}
		if page.NextToken == nil {
			break
		}
		nextToken = page.NextToken
	}
	return schedules, errs
}
