//revive:disable:comments-density reason: registry wiring is small and comments would reduce readability.
package resources

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
)

var registeredConstructors = map[string]collectorFactory{
	"eventbridge_rule": func(cfg *aws.Config, region string) (Collector, error) {
		collector, err := NewEventBridgeCollector(cfg, region)
		if err != nil {
			return nil, fmt.Errorf("new eventbridge collector: %w", err)
		}
		return collector, nil
	},
	"eventbridge_scheduler": func(cfg *aws.Config, region string) (Collector, error) {
		collector, err := NewSchedulerCollector(cfg, region)
		if err != nil {
			return nil, fmt.Errorf("new scheduler collector: %w", err)
		}
		return collector, nil
	},
}

// collectorFactory is a function type that creates a Collector given an AWS config and region.
type collectorFactory func(*aws.Config, string) (Collector, error)

// initializeCollectors instantiates all registered collectors for a region.
func initializeCollectors(cfg *aws.Config, region string) ([]Collector, error) {
	collectors := make([]Collector, 0, len(registeredConstructors))
	for name, factory := range registeredConstructors {
		collector, err := factory(cfg, region)
		if err != nil {
			return nil, fmt.Errorf("initialize collector %s for %s: %w", name, region, err)
		}
		collectors = append(collectors, collector)
	}
	return collectors, nil
}
