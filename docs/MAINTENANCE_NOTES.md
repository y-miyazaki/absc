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
- [internal/aws/resources/runs/collector_redshift.go](internal/aws/resources/runs/collector_redshift.go)
- [internal/aws/resources/runs/collector_step_functions.go](internal/aws/resources/runs/collector_step_functions.go)

## Cache Structure (Current)

### Current State
The run collector cache structure in [internal/aws/resources/runs/collector_common.go](internal/aws/resources/runs/collector_common.go) now uses service-keyed maps:
- Run caches are organized as `service -> cacheKey -> runs`
- Error caches are organized as `service -> cacheKey -> error`
- CloudTrail event caches remain separated because they have different value types
- Service cache maps are lazily initialized on first access

### Current Implementation
```go
type runCollectorCaches struct {
    runResultsCache map[string]map[string][]resourcescore.Run // service -> cacheKey -> runs
    runErrCache     map[string]map[string]error                // service -> cacheKey -> error
    cloudTrailEventsCache map[string][]cloudtrailtypes.Event
    cloudTrailEventErrCache map[string]error
}

// ensureServiceRunCaches() lazily creates service-level maps.
// getCachedRunsForCollector() resolves service name via collector.Service().
```

### Service Name Source
Collector call sites should use [collector.Service()](internal/aws/resources/runs/collector_common.go) through `getCachedRunsForCollector(...)` instead of manually passing service strings. This avoids typo-prone string literals at each call site.

### Adding a New Service (Current Process)
When adding support for a new target service (e.g., "target-service"):

1. **Create collector** (e.g., `target_service.go`) implementing `runCollector` interface.
2. **Use `getCachedRunsForCollector(...)`** in `Collect()` to reuse service-keyed caches.
3. **Register collector** in [resolver.go](internal/aws/resources/runs/resolver.go) registrations.
4. **Add tests** following [collector_lambda_test.go](internal/aws/resources/runs/collector_lambda_test.go) pattern.
5. **Update observable kinds** if needed: add the new service to the `observableTargetKinds` map in [internal/exporter/cron.go](internal/exporter/cron.go).

### Service Onboarding Checklist
Use this checklist to avoid partial implementations when introducing a new scheduler target service.

1. **Runs collector**: add `collector_<service>.go` in [internal/aws/resources/runs](internal/aws/resources/runs) and register it in [internal/aws/resources/runs/resolver.go](internal/aws/resources/runs/resolver.go).
2. **Target hints**: if CloudTrail filtering needs IDs, add fields to `runs.TargetHints` in [internal/aws/resources/runs/resolver.go](internal/aws/resources/runs/resolver.go) and populate them from scheduler input.
3. **Scheduler resolver**: add/extend service entry in `schedulerSDKResolvers` in [internal/aws/resources/scheduler.go](internal/aws/resources/scheduler.go) (`runTarget` and `displayName`).
4. **Target classification**: update kind/service/action detection in [internal/aws/resources/utils.go](internal/aws/resources/utils.go) so schedules are routed to the right collector.
5. **Exporter display/linking**: add UI behavior and console URL mapping in [internal/exporter/html_template.html](internal/exporter/html_template.html) when required.
6. **Tests**: add coverage for resolver/displayName and kind/action detection in [internal/aws/resources/scheduler_target_name_test.go](internal/aws/resources/scheduler_target_name_test.go) and [internal/aws/resources/utils_test.go](internal/aws/resources/utils_test.go).
7. **Runtime verification**: run collection and confirm expected schedules have non-zero `runs` in `output/<account>/schedules/schedules.json`.

## Recent Runs Refactoring (EC2/RDS/Redshift)

### What Was Unified
The following common flow is now shared across EC2, RDS, and Redshift collectors:

1. CloudTrail action lookup
2. Resource-based filtering
3. Run-object assembly from CloudTrail event metadata

Key shared helpers:
- [collectCloudTrailFilteredRuns](internal/aws/resources/runs/collector_common.go) in [internal/aws/resources/runs/collector_common.go](internal/aws/resources/runs/collector_common.go)
- [cloudTrailResourceIDsFromMap](internal/aws/resources/runs/support_cloudtrail.go), [cloudTrailResponseStateFromMap](internal/aws/resources/runs/support_cloudtrail.go), and [cloudTrailRunFromEvent](internal/aws/resources/runs/support_cloudtrail.go) in [internal/aws/resources/runs/support_cloudtrail.go](internal/aws/resources/runs/support_cloudtrail.go)

### What Remains Service-Specific
Only service-specific behavior should remain in each collector:

- Request/response field keys used to extract resource identifiers
- Event-to-status mapping (`runStatus`)
- Fallback resource extraction rules when request fields are absent

This keeps collectors consistent while preserving per-service semantics.

## Responsibility Boundary: Runs vs Exporter Links

### Current Rule
- `runs` layer: collects execution history and normalizes run data
- `exporter` layer: builds AWS Console URLs for HTML output

Relevant link generation implementation:
- [internal/exporter/html_template.html](internal/exporter/html_template.html)

### Why Links Stay in Exporter
- Console URL formats are presentation concerns and can change independently of run collection logic.
- Keeping URL construction in exporter prevents UI-specific rules from leaking into data collection code.
- The runs layer remains reusable for non-HTML outputs.

### Practical Guidance
If link generation becomes too complex, extract a dedicated exporter-side URL builder, but do not move URL logic into run collectors.

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
