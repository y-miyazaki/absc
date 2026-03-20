# Maintenance Notes

## Current Cron Parsing Architecture

Cron parsing and matching logic is now centralized in [internal/helpers/aws_cron.go](internal/helpers/aws_cron.go).

The following packages delegate to the shared helper functions:

- [internal/aws/resources/scheduler_next_invocation.go](internal/aws/resources/scheduler_next_invocation.go)
- [internal/exporter/cron.go](internal/exporter/cron.go)

This removed prior duplication and eliminated the need for a separate cron utility package proposal.

## Current Resource Collection Architecture

The resources package was split into a small shared-core layer and service-specific run collectors.

Shared types and collection options:
- [internal/aws/resources/core/types.go](internal/aws/resources/core/types.go)

Run collection implementations:
- [internal/aws/resources/runs/resolver.go](internal/aws/resources/runs/resolver.go)
- [internal/aws/resources/runs/batch.go](internal/aws/resources/runs/batch.go)
- [internal/aws/resources/runs/ecs.go](internal/aws/resources/runs/ecs.go)
- [internal/aws/resources/runs/ecs_cloudtrail.go](internal/aws/resources/runs/ecs_cloudtrail.go)
- [internal/aws/resources/runs/glue_step.go](internal/aws/resources/runs/glue_step.go)
- [internal/aws/resources/runs/lambda.go](internal/aws/resources/runs/lambda.go)

Legacy files such as `collect_batch.go`, `collect_ecs.go`, and `collect_lambda.go` were removed as part of this refactor.

## Change Log

### 2026-03-19: Restored Lambda maxResults Guard

Issue:
- In Lambda run collection, the max-results guard had been removed from pattern-based log collection, which could cause over-collection from CloudWatch Logs.

Current behavior:
- [internal/aws/resources/runs/lambda.go](internal/aws/resources/runs/lambda.go) enforces the cap in `collectLambdaRunsWithPattern()`.

```go
if len(runs) >= maxResults {
        return runs, nil
}
```

Impact:
- Prevents unbounded memory growth for high-volume functions.
- Keeps behavior consistent with other collectors that enforce `maxResults`.

## Test Coverage Notes

### Existing Cron-Related Regression Tests

- `TestComputeSchedulerNextInvocation_CronWraparoundMinuteRange`
- `TestComputeSchedulerNextInvocation_CronWraparoundWeekdayRange`
- `TestBuildOutput_SlotRunIssues_WraparoundMinuteRange_Issue`
- `TestBuildOutput_SlotRunIssues_WraparoundWeekdayOnWindowDay_Issue`
- `TestBuildSlots_CronWraparoundHourRange`
- `TestBuildSlots_CronWraparoundMinuteStepRange`

### Suggested Additional Tests

1. Add focused tests for scheduler target-input parsing in [internal/aws/resources/scheduler.go](internal/aws/resources/scheduler.go), especially `resolveSchedulerRunTarget()` for `aws-sdk:ecs:*` payload variants.
2. Expand edge-case cron tests for wider year ranges and mixed wrapping ranges in one expression.
