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
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/y-miyazaki/absc/internal/aws/resources/runs"
)

type SchedulerCollector struct {
	batchSvc *batch.Client
	ctSvc    *cloudtrail.Client
	cwlSvc   *cloudwatchlogs.Client
	ecsSvc   *ecs.Client
	glueSvc  *glue.Client
	svc      *scheduler.Client
	stepSvc  *sfn.Client
	region   string
}

type runTargetResolution struct {
	hints              runs.TargetHints
	runJobName         string
	runTargetARN       string
	hasBatchParameters bool
}

// NewSchedulerCollector builds regional clients for EventBridge Scheduler.
func NewSchedulerCollector(cfg *aws.Config, region string) (*SchedulerCollector, error) {
	return &SchedulerCollector{
		region:   region,
		svc:      scheduler.NewFromConfig(*cfg, func(o *scheduler.Options) { o.Region = region }),
		stepSvc:  sfn.NewFromConfig(*cfg, func(o *sfn.Options) { o.Region = region }),
		batchSvc: batch.NewFromConfig(*cfg, func(o *batch.Options) { o.Region = region }),
		ctSvc:    cloudtrail.NewFromConfig(*cfg, func(o *cloudtrail.Options) { o.Region = region }),
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
//
//nolint:gocritic // CollectOptions is intentionally passed by value to preserve the public API.
func (c *SchedulerCollector) Collect(ctx context.Context, opts CollectOptions) ([]Schedule, []ErrorRecord) {
	schedules := make([]Schedule, 0)
	errs := make([]ErrorRecord, 0)
	nowUTC := time.Now().UTC()
	resolver := runs.NewResolver(c.region, c.stepSvc, c.batchSvc, c.ctSvc, c.ecsSvc, c.glueSvc, c.cwlSvc)

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
			hints := runs.TargetHints{}
			if detail.Target != nil {
				targetARN = aws.ToString(detail.Target.Arn)
				targetInput := aws.ToString(detail.Target.Input)
				resolved := resolveSchedulerRunTarget(targetARN, targetInput)
				runTargetARN = resolved.runTargetARN
				runJobName = resolved.runJobName
				hasBatchParameters = resolved.hasBatchParameters
				hints = resolved.hints
				hints.ECSRoleARN = aws.ToString(detail.Target.RoleArn)
				if runTargetARN == "" {
					runTargetARN = targetARN
				}
				targetKind := detectTargetKind(targetARN, hasBatchParameters)
				targetService := detectTargetService(targetARN)
				targetAction := detectTargetAction(targetARN)
				targetName := resolveSchedulerTargetName(targetARN, targetInput, runTargetARN)
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
					TargetAction:               targetAction,
					TargetService:              targetService,
					TargetName:                 targetName,
					NextInvocationAt:           nextInvocationAt,
					Slots:                      buildSlots(aws.ToString(detail.ScheduleExpression)),
					Runs:                       make([]Run, 0),
				}
				if runErr := resolver.PopulateScheduleRuns(ctx, &s, runTargetARN, runJobName, hints, opts); runErr != nil {
					errs = append(errs, *runErr)
				}
				schedules = append(schedules, s)
				continue
			}
			targetKind := detectTargetKind(targetARN, hasBatchParameters)
			targetService := detectTargetService(targetARN)
			targetName := resolveSchedulerTargetName(targetARN, "", runTargetARN)
			targetAction := detectTargetAction(targetARN)
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
				TargetAction:               targetAction,
				TargetService:              targetService,
				TargetName:                 targetName,
				NextInvocationAt:           nextInvocationAt,
				Slots:                      buildSlots(aws.ToString(detail.ScheduleExpression)),
				Runs:                       make([]Run, 0),
			}
			if runErr := resolver.PopulateScheduleRuns(ctx, &s, runTargetARN, runJobName, hints, opts); runErr != nil {
				errs = append(errs, *runErr)
			}
			schedules = append(schedules, s)
		}
	}
	return schedules, errs
}

// sdkTargetResolver bundles all per-service resolution logic for aws-sdk scheduler targets.
// runTarget extracts the downstream run target (for run history lookup); nil for terminal resources.
// displayName extracts the human-readable resource name; nil means fall back to resourceNameFromARN.
type sdkTargetResolver struct {
	runTarget   func(string) runTargetResolution
	displayName func(string, string) (string, bool)
}

// schedulerSDKResolvers maps an AWS SDK service name (lowercase) to its per-service resolver.
// Add a new entry here to support additional aws-sdk target services.
var schedulerSDKResolvers = map[string]sdkTargetResolver{
	"batch": {
		runTarget: func(input string) runTargetResolution {
			return runTargetResolution{
				runTargetARN:       getStringFromJSON(input, "JobQueue"),
				runJobName:         getStringFromJSON(input, "JobName"),
				hasBatchParameters: true,
			}
		},
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = runTargetARN
			v := getStringFromJSON(input, "JobName")
			return v, v != ""
		},
	},
	"ec2": {
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = runTargetARN
			ids := getStringSliceFromJSON(input, "InstanceIds")
			if len(ids) == 1 {
				return ids[0], true
			}
			return "", false
		},
	},
	"ecs": {
		runTarget: func(input string) runTargetResolution {
			resolved := runTargetResolution{}
			if v := getStringFromJSON(input, "Cluster"); v != "" {
				resolved.runTargetARN = v
			}
			resolved.hints.ECSTaskDefinitionARN = getStringFromJSON(input, "TaskDefinition")
			resolved.hints.ECSStartedBy = getStringFromJSON(input, "StartedBy")
			return resolved
		},
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = input
			if runTargetARN != "" {
				return resourceNameFromARN(runTargetARN), true
			}
			return "", false
		},
	},
	"glue": {
		runTarget: func(input string) runTargetResolution {
			if v := getStringFromJSON(input, "JobName"); v != "" {
				return runTargetResolution{runTargetARN: v}
			}
			return runTargetResolution{}
		},
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = input
			if runTargetARN != "" {
				return resourceNameFromARN(runTargetARN), true
			}
			return "", false
		},
	},
	"lambda": {
		runTarget: func(input string) runTargetResolution {
			if v := getStringFromJSON(input, "FunctionName"); v != "" {
				return runTargetResolution{runTargetARN: v}
			}
			return runTargetResolution{}
		},
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = input
			if runTargetARN != "" {
				return resourceNameFromARN(runTargetARN), true
			}
			return "", false
		},
	},
	"rds": {
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = runTargetARN
			v := getStringFromJSON(input, "DbClusterIdentifier", "DbInstanceIdentifier")
			return v, v != ""
		},
	},
	"redshift": {
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = runTargetARN
			v := getStringFromJSON(input, "ClusterIdentifier", "WorkgroupName")
			return v, v != ""
		},
	},
	"sfn": {
		runTarget: func(input string) runTargetResolution {
			if v := getStringFromJSON(input, "StateMachineArn"); v != "" {
				return runTargetResolution{runTargetARN: v}
			}
			return runTargetResolution{}
		},
		displayName: func(input, runTargetARN string) (string, bool) {
			_ = input
			if runTargetARN != "" {
				return resourceNameFromARN(runTargetARN), true
			}
			return "", false
		},
	},
}

// awsSDKServiceFromARN extracts the service name from an aws-sdk scheduler target ARN.
// For an ARN containing ":aws-sdk:SERVICE:ACTION", returns "service" (lowercase).
func awsSDKServiceFromARN(lowerARN string) string {
	const marker = ":aws-sdk:"
	idx := strings.Index(lowerARN, marker)
	if idx < 0 {
		return ""
	}
	rest := lowerARN[idx+len(marker):]
	if colon := strings.Index(rest, ":"); colon >= 0 {
		return rest[:colon]
	}
	return rest
}

func resolveSchedulerTargetName(targetARN, input, runTargetARN string) string {
	lowerARN := strings.ToLower(targetARN)
	if svc := awsSDKServiceFromARN(lowerARN); svc != "" {
		if r, ok := schedulerSDKResolvers[svc]; ok && r.displayName != nil {
			if name, extracted := r.displayName(input, runTargetARN); extracted {
				return name
			}
		}
	}
	return resourceNameFromARN(targetARN)
}

// resolveSchedulerRunTarget extracts the real downstream target from SDK inputs.
func resolveSchedulerRunTarget(targetARN, input string) runTargetResolution {
	lowerARN := strings.ToLower(targetARN)
	if svc := awsSDKServiceFromARN(lowerARN); svc != "" {
		if r, ok := schedulerSDKResolvers[svc]; ok && r.runTarget != nil {
			return r.runTarget(input)
		}
		return runTargetResolution{}
	}
	return runTargetResolution{hasBatchParameters: strings.Contains(lowerARN, ":batch:")}
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

func getStringSliceFromJSON(raw string, keys ...string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	for _, key := range keys {
		value, ok := m[key]
		if !ok {
			continue
		}
		array, ok := value.([]any)
		if !ok {
			continue
		}
		result := make([]string, 0, len(array))
		for _, item := range array {
			text, okCast := item.(string)
			if !okCast || strings.TrimSpace(text) == "" {
				continue
			}
			result = append(result, text)
		}
		if len(result) > 0 {
			return result
		}
	}
	return nil
}
