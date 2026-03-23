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
		{name: "service max floor", limit: 10, serviceMax: 0, want: 1},
		{name: "use default when limit is zero", limit: 0, serviceMax: 200, want: 144},
		{name: "use explicit limit", limit: 50, serviceMax: 200, want: 50},
		{name: "clamp to service max", limit: 300, serviceMax: 200, want: 200},
	}

	for _, tt := range tests {
		tt := tt
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
		{name: "service max floor", limit: 100, collected: 0, serviceMax: 0, want: 1},
		{name: "remaining under max", limit: 25, collected: 10, serviceMax: 200, want: 15},
		{name: "remaining over max clamps", limit: 400, collected: 100, serviceMax: 200, want: 200},
		{name: "remaining never below one", limit: 10, collected: 10, serviceMax: 200, want: 1},
		{name: "fallback to default when limit unset", limit: 0, collected: 99, serviceMax: 200, want: 144},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := remainingPageSize(tt.limit, tt.collected, tt.serviceMax)
			if got != tt.want {
				t.Fatalf("remainingPageSize(%d, %d, %d) = %d, want %d", tt.limit, tt.collected, tt.serviceMax, got, tt.want)
			}
		})
	}
}
