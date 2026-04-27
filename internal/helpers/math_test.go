package helpers

import "testing"

func TestSafeInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input int
		want  int32
	}{
		{name: "negative clamps to zero", input: -1, want: 0},
		{name: "in range", input: 12345, want: 12345},
		{name: "over max clamps", input: maxInt32Value + 1, want: maxInt32Value},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SafeInt32(tt.input)
			if got != tt.want {
				t.Fatalf("SafeInt32(%d) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
