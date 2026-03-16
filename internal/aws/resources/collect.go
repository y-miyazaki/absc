package resources

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type CollectOptions struct {
	MaxConcurrency int
	MaxResults     int
	Regions        []string
	Since          time.Time
}

const defaultMaxConcurrency = 5
const defaultMaxResults = 50

type Collector interface {
	Collect(ctx context.Context, opts CollectOptions) ([]Schedule, []ErrorRecord)
	Name() string
}

func init() {
	registerConstructor("eventbridge_rule", NewEventBridgeCollector)
	registerConstructor("eventbridge_scheduler", NewSchedulerCollector)
}

func Collect(ctx context.Context, cfg aws.Config, opts CollectOptions) ([]Schedule, []ErrorRecord) {
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

	jobs := make([]struct {
		collector Collector
	}, 0)

	for _, region := range opts.Regions {
		regionCfg := cfg
		regionCfg.Region = region
		registered, err := initializeCollectors(regionCfg, region)
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

	results := make(chan collectResult, len(jobs))
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(c Collector) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
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
