package resources

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type collectorConstructor any

var (
	registeredConstructors = map[string]collectorConstructor{}
)

func registerConstructor(name string, constructor collectorConstructor) {
	registeredConstructors[name] = constructor
}

func initializeCollectors(cfg aws.Config, region string) ([]Collector, error) {
	collectors := make([]Collector, 0, len(registeredConstructors))
	for name, constructor := range registeredConstructors {
		collector, err := createCollector(name, constructor, cfg, region)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, collector)
	}
	return collectors, nil
}

func createCollector(name string, constructor collectorConstructor, cfg aws.Config, region string) (Collector, error) {
	result := reflect.ValueOf(constructor).Call([]reflect.Value{
		reflect.ValueOf(cfg),
		reflect.ValueOf(region),
	})
	if len(result) != 2 {
		return nil, fmt.Errorf("invalid constructor signature for %s", name)
	}
	if !result[1].IsNil() {
		err, ok := result[1].Interface().(error)
		if !ok {
			return nil, fmt.Errorf("invalid error return type for %s", name)
		}
		return nil, fmt.Errorf("failed to initialize collector %s: %w", name, err)
	}
	collector, ok := result[0].Interface().(Collector)
	if !ok {
		return nil, fmt.Errorf("invalid collector return type for %s", name)
	}
	return collector, nil
}
