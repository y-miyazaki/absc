# ABSC Functional Specification

This document records the behavioral specifications of ABSC that are present in the implementation but not described in any other document.

## Processing Pipeline

ABSC follows a three-stage pipeline on each invocation.

```
Collect  →  Build  →  Export
```

**Collect** fans out AWS API calls per region and per collector, merges schedules and soft errors, and returns them to the caller. Partial failures from one region or collector do not stop the full run.

**Build** accepts the collected schedules and transforms them into the output domain. It anchors the timeline window, remaps cron slots from the source timezone to the display timezone, filters and classifies runs, and emits slot issues.

**Export** writes four output files from the built result: a JSON payload, an HTML timeline viewer, an errors HTML page, and a slot issue CSV.

## CLI Defaults

The following constants are applied when a flag is omitted.

| Flag                      | Default          | Notes                                   |
| ------------------------- | ---------------- | --------------------------------------- |
| `--region`                | `ap-northeast-1` | Any single valid region                 |
| `--timezone`              | `UTC`            | IANA location name                      |
| `--lookback-hours`        | `24`             | Positive integer                        |
| `--max-concurrency`       | `5`              | Bounded semaphore size                  |
| `--max-results`           | `144`            | Equals `SlotsPerDay`                    |
| `--output-dir`            | `./output`       | Relative path is allowed                |
| `--timeout`               | `10m`            | Applies to full run including AWS calls |
| `--include-non-slot-runs` | `false`          | See Run Alignment section               |

`--max-results` defaults to 144 because it matches `SlotsPerDay`. One run per slot is the natural upper bound under normal schedule conditions.

## Timeline Window Model

The timeline window is a single 24-hour calendar day anchored to the display timezone.

The computation proceeds as follows.

1. `since = now - lookback-hours`, converted to the display timezone.
2. The window start is the calendar-day start of `since` in the display timezone (hour 0, minute 0, second 0).
3. The window end is always `window-start + 24 hours`.
4. `until` passed to collectors is fixed at `window-start + 1 day` regardless of `lookback-hours`.

With the default 24-hour lookback the window covers the previous full calendar day in the display timezone. Increasing `lookback-hours` beyond 24 shifts `since` further back, which moves the window start to an earlier calendar day.

The run enrichment query range is defined by `since` and `until`, not by `now` and `now - lookback-hours`. Collectors receive only the clamped window interval.

## Slot Model

Each schedule carries a `slots` array of 144 integers, one per 10-minute slot in the 24-hour window.

- A value of `1` means a scheduled invocation is expected within that slot.
- A value of `0` means no invocation is expected in that slot.

The slot granularity constants are:

- `SlotsPerDay = 144`
- `minutesPerSlot = 10`
- `slotsPerHour = 6`

Slots are first computed in the timezone of the schedule expression (see `ScheduleExpressionTimezone`). They are then remapped to the display timezone by the Build stage before being written to the output. This means the `slots` array in the JSON output is always relative to the display timezone window start.

## Schedule Collection Model

### Sources

ABSC collects schedules from two sources.

- EventBridge Rules — only rules with schedule expressions are collected.
- EventBridge Scheduler — all schedules including one-time `at()` expressions.

### Concurrency

The collector fan-out is bounded by a semaphore of size `max-concurrency`. One goroutine is started per collector per region. The semaphore prevents more than `max-concurrency` goroutines from executing AWS API calls simultaneously.

If a collector returns an error, that error is recorded in `ErrorRecord` and collection continues for the remaining collectors and regions.

### Disabled Schedules

Disabled schedules are included in the output. They still carry runs collected within the lookback window. Slot issues are not emitted for disabled schedules.

## Run Enrichment

Run enrichment is the optional phase where collectors fetch recent execution history and attach it to each schedule.

Observable target kinds that support run enrichment are defined in [internal/exporter/slot_issue_policy.json](internal/exporter/slot_issue_policy.json):

- `batch`
- `ecs`
- `glue`
- `lambda`
- `stepfunctions`

For all other target kinds, the schedule is displayed with empty runs. The `run_in_slot_category` field is set to `not_observable_target` in that case.

### maxResults

Each collector enforces `maxResults` independently. When a collector returns exactly `maxResults` records, the schedule is marked `runs_capped = true`. This flag is exposed to callers and propagated to the slot issue reason.

### ECS Backfill

ECS stopped task history from the ECS API is limited to approximately 1 hour after the task stops. For longer lookback windows, the collector supplements results by reading CloudTrail management events with event source `ecs.amazonaws.com` and event name `RunTask`.

When both sources return entries for the same task, ECS API records are preferred over CloudTrail-only records (merge policy: ECS record wins).

## Output Files

The Build stage produces four files written under `{output-dir}/{account-id}/schedules/`.

| File                  | Content                                                 |
| --------------------- | ------------------------------------------------------- |
| `index.html`          | Self-contained schedule timeline viewer                 |
| `schedules.json`      | Machine-readable JSON payload                           |
| `errors.html`         | HTML table of soft errors keyed by service and region   |
| `slot_run_issues.csv` | CSV rows for each slot issue (see Slot Issue Detection) |

The JSON schema version is `1.0`. This value is stored in the `version` field of the output.

Error records are deduplicated within a single run. The deduplication key is the concatenation of `service`, `region`, and `message`.

## Slot Issue Detection

Slot issues identify scheduled slots where no matching run was observed. They are recorded per schedule and per slot.

### Conditions for Emission

Slot issues are emitted only when all of the following conditions are met.

- The schedule is enabled (`enabled = true`).
- The target kind is observable (`run_in_slot_category = observable`).
- The schedule is expected to fire within the window (`expected_in_window = true`).

### Issue Reasons

Each slot issue carries one of three reason codes.

| Reason                    | When Assigned                                                |
| ------------------------- | ------------------------------------------------------------ |
| `NO_RUN_IN_WINDOW`        | No runs at all were collected for the schedule in the window |
| `COLLECTED_RUNS_CAPPED`   | `runs_capped = true`; run history may be incomplete          |
| `RUN_NOT_ALIGNED_TO_SLOT` | Runs exist but none overlaps this scheduled slot             |

The reason is selected once per schedule and applied uniformly to every slot issue in that schedule. `COLLECTED_RUNS_CAPPED` takes precedence over `RUN_NOT_ALIGNED_TO_SLOT`.

### run_in_slot_category

Every schedule in the output carries a `run_in_slot_category` field.

| Value                   | Meaning                                                                  |
| ----------------------- | ------------------------------------------------------------------------ |
| `observable`            | Target kind supports run enrichment and schedule fires in the window     |
| `not_observable_target` | Target kind does not support run enrichment                              |
| `not_scheduled_today`   | Target kind is observable but the expression does not fire in the window |

Slot issues are only emitted when `run_in_slot_category = observable`.

## Run Alignment

In the Build stage, every run collected within the display window is tested against the `slots` array.

A run is **aligned** if its time interval overlaps at least one slot that carries `1` (a scheduled slot). A run is **misaligned** if it overlaps no scheduled slot.

By default, only aligned runs are included in the per-schedule `runs` array in the output. Misaligned runs are recorded separately in the top-level `alignment_issues` array with the reason `"run does not overlap any scheduled slot in the displayed window"`.

When `--include-non-slot-runs` is set, all window runs are included in the per-schedule `runs` array regardless of slot alignment. The `alignment_issues` array is still populated with misaligned entries.

## Expected Window Detection

The `expected_in_window` field indicates whether the schedule expression is expected to fire within the display window.

For `cron()` expressions, detection works by scanning every minute from the window start to the window end in the schedule expression timezone and testing the cron expression against each candidate. If any minute matches, `expected_in_window = true`.

For `rate()` and `at()` expressions, `expected_in_window` is always `true` because rate intervals are assumed to be continuous and one-time expressions are assumed to fall within any given day.

If the cron expression cannot be parsed (wrong field count, unknown syntax), `expected_in_window` defaults to `true` to avoid silently suppressing slot issues for malformed expressions.

## Next Invocation Computation

The `next_invocation_at` field is computed differently for each expression type.

For `at(datetime)` expressions, the datetime is parsed as a one-time candidate. If it falls in the past relative to now, the field is empty.

For `rate(value unit)` expressions, the interval is computed from the schedule creation date forward until a future candidate is found.

For `cron()` expressions, the next invocation is found by advancing minute-by-minute from now in the schedule expression timezone until the cron fields match. The search is bounded to 5 years forward from now. If an end date is configured on the schedule and the search exhausts candidates before reaching it, the field is empty.

Next invocation is only computed for enabled schedules. Disabled schedules always have an empty `next_invocation_at`.
