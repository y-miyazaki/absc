// Package resources collects AWS schedules and recent execution history.
package resources

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type CollectOptions struct {
	Since          time.Time
	Regions        []string
	MaxConcurrency int
	MaxResults     int
}

const defaultMaxConcurrency = 5
const defaultMaxResults = 50

// Collector is implemented by each AWS-backed schedule source.
type Collector interface {
	// Collect returns schedules and soft errors for the collector's target service.
	Collect(ctx context.Context, opts CollectOptions) ([]Schedule, []ErrorRecord) //nolint:unparam,unused // Interface signatures need parameter names for readability.
	// Name returns the stable collector identifier used in output and errors.
	Name() string
}

// Collect fans out per-region collectors and merges their schedules and errors.
// It keeps partial failures so one region or service does not stop the full run.
// Default concurrency and max-results values are applied when callers omit them.
func Collect(ctx context.Context, cfg *aws.Config, opts CollectOptions) ([]Schedule, []ErrorRecord) {
	schedules := make([]Schedule, 0)
	errs := make([]ErrorRecord, 0)
	concurrency := opts.MaxConcurrency
	if concurrency <= 0 {
		concurrency = defaultMaxConcurrency
	}
	if opts.MaxResults < 1 {
		opts.MaxResults = defaultMaxResults
	}

	type collectResult struct {
		schedules []Schedule
		errs      []ErrorRecord
	}

	// Build one job per collector per region.
	jobs := make([]struct {
		collector Collector
	}, 0)

	for _, region := range opts.Regions {
		regionCfg := *cfg
		regionCfg.Region = region
		registered, err := initializeCollectors(&regionCfg, region)
		if err != nil {
			errs = append(errs, ErrorRecord{Service: "collector_init", Region: region, Message: err.Error()})
			continue
		}

		for _, collector := range registered {
			jobs = append(jobs, struct {
				collector Collector
			}{collector: collector})
		}
	}

	if len(jobs) == 0 {
		return schedules, errs
	}

	// Run collectors with a bounded semaphore.
	results := make(chan collectResult, len(jobs))
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(c Collector) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			// Each collector reports partial failures through its own error records.
			res, colErrs := c.Collect(ctx, opts)
			results <- collectResult{schedules: res, errs: colErrs}
		}(job.collector)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		schedules = append(schedules, result.schedules...)
		errs = append(errs, result.errs...)
	}

	return schedules, errs
}
