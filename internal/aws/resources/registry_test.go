//revive:disable:comments-density reason: registry wiring is validated by key presence only.
package resources

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestRegisteredConstructors(t *testing.T) {
	t.Parallel()

	want := []string{"eventbridge_rule", "eventbridge_scheduler"}
	if got, wantLen := len(registeredConstructors), len(want); got != wantLen {
		t.Fatalf("len(registeredConstructors) = %d, want %d", got, wantLen)
	}
	for _, name := range want {
		if _, ok := registeredConstructors[name]; !ok {
			t.Fatalf("registeredConstructors missing %q", name)
		}
	}
}

func TestInitializeCollectors(t *testing.T) {
	t.Parallel()

	cfg := &aws.Config{Region: "ap-northeast-1"}
	collectors, err := initializeCollectors(cfg, "us-east-1")
	if err != nil {
		t.Fatalf("initializeCollectors() error = %v", err)
	}
	if got, want := len(collectors), len(registeredConstructors); got != want {
		t.Fatalf("len(collectors) = %d, want %d", got, want)
	}
	for _, collector := range collectors {
		if collector.Name() == "" {
			t.Fatal("collector.Name() = empty, want non-empty")
		}
	}
}
