//revive:disable:comments-density reason: registry wiring is validated by key presence only.
package resources

import "testing"

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
