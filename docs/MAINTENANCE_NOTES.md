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
- [internal/aws/resources/runs/collector_batch.go](internal/aws/resources/runs/collector_batch.go)
- [internal/aws/resources/runs/collector_ec2.go](internal/aws/resources/runs/collector_ec2.go)
- [internal/aws/resources/runs/collector_ecs.go](internal/aws/resources/runs/collector_ecs.go)
- [internal/aws/resources/runs/collector_glue.go](internal/aws/resources/runs/collector_glue.go)
- [internal/aws/resources/runs/collector_lambda.go](internal/aws/resources/runs/collector_lambda.go)
- [internal/aws/resources/runs/collector_rds.go](internal/aws/resources/runs/collector_rds.go)
- [internal/aws/resources/runs/collector_step_functions.go](internal/aws/resources/runs/collector_step_functions.go)

## Cache Structure Refactoring Opportunity

### Current State
The run collector cache structure in [internal/aws/resources/runs/support_cache.go](internal/aws/resources/runs/support_cache.go) uses a repetitive pattern:
- 16 map fields for 8 services × 2 caches (error + runs)
- Each service pair follows: `{service}ErrCache`, `{service}RunsCache`
- Constructor (`newRunCollectorCaches()`) requires manual updates for each service

**Services with separate caches:**
- batch, cloudtrail, ec2, ecs, glue, lambda, rds, stepfunctions

### Design Issue
- **Cost of Adding New Service**: Requires 2 struct fields + 2 make() calls
- **DRY Violation**: Cache pattern repeats 8 times
- **Memory Inefficiency**: All caches are pre-allocated even if unused
- **Error-Prone**: Easy to forget updating struct and constructor simultaneously

### Recommended Solution (Deferred)
**Option 1 (Preferred): Generic Map-of-Maps**
```go
type runCollectorCaches struct {
    runs map[string]map[string][]resourcescore.Run     // service -> cacheKey -> runs
    errs map[string]map[string]error                    // service -> cacheKey -> error
    events map[string][]cloudtrailtypes.Event           // cloudtrail-specific (keep separate)
}

func newRunCollectorCaches() *runCollectorCaches {
    return &runCollectorCaches{
        runs:   make(map[string]map[string][]resourcescore.Run),
        errs:   make(map[string]map[string]error),
        events: make(map[string][]cloudtrailtypes.Event),
    }
}

func (c *runCollectorCaches) getRuns(service, key string) ([]resourcescore.Run, error) {
    if _, ok := c.runs[service]; !ok {
        c.runs[service] = make(map[string][]resourcescore.Run)
    }
    return c.runs[service][key], nil
}
```

**Option 2 (Simple): Getter Functions**
```go
func (c *runCollectorCaches) getRunsCache(service string) map[string][]resourcescore.Run {
    switch service {
    case "batch":
        return c.batchRunsCache
    case "ec2":
        return c.ec2RunsCache
    // ...
    }
    return nil
}
```

### Adding a New Service (Current Process)
When adding support for a new target service (e.g., "target-service"):

1. **Update struct** in [support_cache.go](internal/aws/resources/runs/support_cache.go):
   ```go
   type runCollectorCaches struct {
       // ... existing caches ...
       targetServiceErrCache map[string]error
       targetServiceRunsCache map[string][]resourcescore.Run
   }
   ```

2. **Update constructor** `newRunCollectorCaches()`:
   ```go
   return &runCollectorCaches{
       // ... existing makes ...
       targetServiceErrCache: make(map[string]error),
       targetServiceRunsCache: make(map[string][]resourcescore.Run),
   }
   ```

3. **Create collector** (e.g., `target_service.go`) implementing `runCollector` interface
4. **Register collector** in [resolver.go](internal/aws/resources/runs/resolver.go) `NewResolver()`
5. **Add tests** following [collector_lambda_test.go](internal/aws/resources/runs/collector_lambda_test.go) pattern
6. **Update observable kinds** if needed: add the new service to the `observableTargetKinds` map in [internal/exporter/cron.go](internal/exporter/cron.go)

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
