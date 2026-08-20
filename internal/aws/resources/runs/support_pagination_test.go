//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import "testing"

func TestPageSizeForLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		serviceMax int32
		want       int32
	}{
		{name: "service max floor", limit: testPageLimit10, serviceMax: 0, want: testPageLimit1},
		{name: "use service max when limit is zero", limit: 0, serviceMax: testPageLimit200, want: testPageLimit200},
		{name: "use explicit limit", limit: testPageLimit50, serviceMax: testPageLimit200, want: testPageLimit50},
		{name: "clamp to service max", limit: testPageLimit300, serviceMax: testPageLimit200, want: testPageLimit200},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := pageSizeForLimit(tt.limit, tt.serviceMax)
			if got != tt.want {
				t.Fatalf("pageSizeForLimit(%d, %d) = %d, want %d", tt.limit, tt.serviceMax, got, tt.want)
			}
		})
	}
}

func TestRemainingPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		collected  int
		serviceMax int32
		want       int32
	}{
		{name: "service max floor", limit: testPageLimit100, collected: 0, serviceMax: 0, want: testPageLimit1},
		{name: "remaining under max", limit: testPageLimit25, collected: testPageLimit10, serviceMax: testPageLimit200, want: testPageLimit15},
		{name: "remaining over max clamps", limit: testPageLimit400, collected: testPageLimit100, serviceMax: testPageLimit200, want: testPageLimit200},
		{name: "remaining never below one", limit: testPageLimit10, collected: testPageLimit10, serviceMax: testPageLimit200, want: testPageLimit1},
		{name: "fallback to service max when limit unset", limit: 0, collected: testPageLimit99, serviceMax: testPageLimit200, want: testPageLimit200},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := remainingPageSize(tt.limit, tt.collected, tt.serviceMax)
			if got != tt.want {
				t.Fatalf("remainingPageSize(%d, %d, %d) = %d, want %d", tt.limit, tt.collected, tt.serviceMax, got, tt.want)
			}
		})
	}
}
