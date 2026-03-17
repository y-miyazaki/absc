//revive:disable:comments-density reason: collector flow mirrors AWS pagination and detailed comments add noise.
package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

type SchedulerCollector struct {
	batchSvc *batch.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	svc      *scheduler.Client
	stepSvc  *sfn.Client
	region   string
}

// NewSchedulerCollector builds regional clients for EventBridge Scheduler.
func NewSchedulerCollector(cfg *aws.Config, region string) (*SchedulerCollector, error) {
	return &SchedulerCollector{
		region:   region,
		svc:      scheduler.NewFromConfig(*cfg, func(o *scheduler.Options) { o.Region = region }),
		stepSvc:  sfn.NewFromConfig(*cfg, func(o *sfn.Options) { o.Region = region }),
		batchSvc: batch.NewFromConfig(*cfg, func(o *batch.Options) { o.Region = region }),
		cwlSvc:   cloudwatchlogs.NewFromConfig(*cfg, func(o *cloudwatchlogs.Options) { o.Region = region }),
		ecsSvc:   ecs.NewFromConfig(*cfg, func(o *ecs.Options) { o.Region = region }),
		glueSvc:  glue.NewFromConfig(*cfg, func(o *glue.Options) { o.Region = region }),
	}, nil
}

// Name returns the collector identifier.
func (*SchedulerCollector) Name() string {
	return "eventbridge_scheduler"
}

// Collect lists schedules, computes next invocations, and resolves recent runs.
func (c *SchedulerCollector) Collect(ctx context.Context, opts CollectOptions) ([]Schedule, []ErrorRecord) {
	schedules := make([]Schedule, 0)
	errs := make([]ErrorRecord, 0)
	nowUTC := time.Now().UTC()
	deps := runCollectorDeps{
		region:   c.region,
		stepSvc:  c.stepSvc,
		batchSvc: c.batchSvc,
		ecsSvc:   c.ecsSvc,
		glueSvc:  c.glueSvc,
		cwlSvc:   c.cwlSvc,
	}
	caches := newRunCollectorCaches()

	p := scheduler.NewListSchedulesPaginator(c.svc, &scheduler.ListSchedulesInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			errs = append(errs, ErrorRecord{Service: "eventbridge_scheduler", Region: c.region, Message: err.Error()})
			return schedules, errs
		}
		for scheduleIndex := range page.Schedules {
			sum := page.Schedules[scheduleIndex]
			detail, getScheduleErr := c.svc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: sum.Name, GroupName: sum.GroupName})
			if getScheduleErr != nil {
				errs = append(errs, ErrorRecord{Service: "eventbridge_scheduler", Region: c.region, Message: getScheduleErr.Error()})
				continue
			}
			enabled := strings.EqualFold(string(detail.State), "ENABLED")
			targetARN := ""
			runTargetARN := ""
			runJobName := ""
			hasBatchParameters := false
			if detail.Target != nil {
				targetARN = aws.ToString(detail.Target.Arn)
				runTargetARN, runJobName, hasBatchParameters = resolveSchedulerRunTarget(targetARN, aws.ToString(detail.Target.Input))
				if runTargetARN == "" {
					runTargetARN = targetARN
				}
			}
			targetKind := detectTargetKind(targetARN, hasBatchParameters)
			targetService := detectTargetService(targetARN)
			nextInvocationAt := computeSchedulerNextInvocation(detail, nowUTC)
			s := Schedule{
				ID:                         fmt.Sprintf("eventbridge_scheduler:%s:%s", c.region, aws.ToString(detail.Name)),
				Service:                    "eventbridge_scheduler",
				ScheduleName:               aws.ToString(detail.Name),
				ScheduleExpression:         aws.ToString(detail.ScheduleExpression),
				ScheduleExpressionTimezone: aws.ToString(detail.ScheduleExpressionTimezone),
				Enabled:                    enabled,
				Region:                     c.region,
				TargetARN:                  targetARN,
				TargetKind:                 targetKind,
				TargetService:              targetService,
				TargetName:                 resourceNameFromARN(targetARN),
				NextInvocationAt:           nextInvocationAt,
				Slots:                      buildSlots(aws.ToString(detail.ScheduleExpression)),
				Runs:                       make([]Run, 0),
			}
			runs, runErr := collectRunsByTargetKind(ctx, targetKind, runTargetARN, runJobName, opts, deps, caches)
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
	return schedules, errs
}

// resolveSchedulerRunTarget extracts the real downstream target from SDK inputs.
func resolveSchedulerRunTarget(targetARN, input string) (runTargetARN, runJobName string, hasBatchParameters bool) {
	lowerARN := strings.ToLower(targetARN)
	if strings.Contains(lowerARN, ":aws-sdk:sfn:startexecution") {
		if v := getStringFromJSON(input, "StateMachineArn", "stateMachineArn", "state_machine_arn"); v != "" {
			return v, "", false
		}
		return "", "", false
	}
	if strings.Contains(lowerARN, ":aws-sdk:batch:submitjob") {
		queue := getStringFromJSON(input, "JobQueue", "jobQueue", "job_queue")
		name := getStringFromJSON(input, "JobName", "jobName", "job_name")
		return queue, name, true
	}
	if strings.Contains(lowerARN, ":aws-sdk:lambda:invoke") {
		if v := getStringFromJSON(input, "FunctionName", "functionName", "function_name"); v != "" {
			return v, "", false
		}
		return "", "", false
	}
	if strings.Contains(lowerARN, ":aws-sdk:glue:") {
		if v := getStringFromJSON(input, "JobName", "jobName", "job_name"); v != "" {
			return v, "", false
		}
		return "", "", false
	}
	if strings.Contains(lowerARN, ":aws-sdk:ecs:") {
		if v := getStringFromJSON(input, "Cluster", "cluster"); v != "" {
			return v, "", false
		}
		return "", "", false
	}
	return "", "", strings.Contains(lowerARN, ":batch:")
}

// getStringFromJSON returns the first matching string field from a JSON object.
func getStringFromJSON(raw string, keys ...string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	for _, key := range keys {
		if value, ok := m[key]; ok {
			if text, okCast := value.(string); okCast {
				return text
			}
		}
	}
	return ""
}
