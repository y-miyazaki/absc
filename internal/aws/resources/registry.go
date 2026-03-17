//revive:disable:comments-density reason: small reflection helper does not benefit from more inline comments.
package resources

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type collectorConstructor any

const collectorResultArity = 2

var (
	errInvalidCollectorSignature = errors.New("invalid constructor signature")
	errInvalidCollectorErrorType = errors.New("invalid error return type")
	errInvalidCollectorType      = errors.New("invalid collector return type")

	registeredConstructors = map[string]collectorConstructor{
		"eventbridge_rule":      NewEventBridgeCollector,
		"eventbridge_scheduler": NewSchedulerCollector,
	}
)

// initializeCollectors instantiates all registered collectors for a region.
func initializeCollectors(cfg *aws.Config, region string) ([]Collector, error) {
	collectors := make([]Collector, 0, len(registeredConstructors))
	for name, constructor := range registeredConstructors {
		collector, err := createCollector(name, constructor, cfg, region)
		if err != nil {
			return nil, fmt.Errorf("initialize collectors for %s: %w", region, err)
		}
		collectors = append(collectors, collector)
	}
	return collectors, nil
}

// createCollector validates the reflected constructor result before use.
func createCollector(name string, constructor collectorConstructor, cfg *aws.Config, region string) (Collector, error) {
	result := reflect.ValueOf(constructor).Call([]reflect.Value{
		reflect.ValueOf(cfg),
		reflect.ValueOf(region),
	})
	if len(result) != collectorResultArity {
		return nil, fmt.Errorf("%w for %s", errInvalidCollectorSignature, name)
	}
	if !result[1].IsNil() {
		err, ok := result[1].Interface().(error)
		if !ok {
			return nil, fmt.Errorf("%w for %s", errInvalidCollectorErrorType, name)
		}
		return nil, fmt.Errorf("failed to initialize collector %s: %w", name, err)
	}
	collector, ok := result[0].Interface().(Collector)
	if !ok {
		return nil, fmt.Errorf("%w for %s", errInvalidCollectorType, name)
	}
	return collector, nil
}
