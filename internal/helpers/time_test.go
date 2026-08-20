package helpers

import (
	"testing"
	"time"
)

func TestConvertRFC3339ToLocation(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("JST", 9*3600)

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "   ", want: ""},
		{name: "invalid", input: "not-rfc3339", want: "not-rfc3339"},
		{name: "valid", input: "2026-04-20T00:00:00Z", want: "2026-04-20T09:00:00+09:00"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ConvertRFC3339ToLocation(tt.input, loc)
			if got != tt.want {
				t.Fatalf("ConvertRFC3339ToLocation(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatRFC3339NanoUTC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tm   time.Time
		want string
	}{
		{name: "zero", tm: time.Time{}, want: ""},
		{
			name: "valid",
			tm:   time.Date(2026, time.April, 20, 1, 2, 3, 456, time.FixedZone("JST", 9*3600)),
			want: "2026-04-19T16:02:03.000000456Z",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := FormatRFC3339NanoUTC(tt.tm)
			if got != tt.want {
				t.Fatalf("FormatRFC3339NanoUTC() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatRFC3339UTC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tm   time.Time
		want string
	}{
		{name: "zero", tm: time.Time{}, want: ""},
		{
			name: "valid",
			tm:   time.Date(2026, time.April, 20, 9, 0, 0, 0, time.FixedZone("JST", 9*3600)),
			want: "2026-04-20T00:00:00Z",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := FormatRFC3339UTC(tt.tm)
			if got != tt.want {
				t.Fatalf("FormatRFC3339UTC() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatUTCOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		want      string
		offsetSec int
	}{
		{name: "positive", offsetSec: 9 * 3600, want: "UTC+09:00"},
		{name: "negative", offsetSec: -(5*3600 + 30*60), want: "UTC-05:30"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := FormatUTCOffset(tt.offsetSec)
			if got != tt.want {
				t.Fatalf("FormatUTCOffset(%d) = %q, want %q", tt.offsetSec, got, tt.want)
			}
		})
	}
}

func TestFromMillisPtrMatchesFromMillis(t *testing.T) {
	t.Parallel()

	v := int64(1713571200000)
	fromValue := FromMillis(v)
	got := FromMillisPtr(&v)
	if !got.Equal(fromValue) {
		t.Fatalf("FromMillisPtr(%d) = %v, want %v", v, got, fromValue)
	}
}

func TestFromMillisAndPtr(t *testing.T) {
	t.Parallel()

	v := int64(1713571200000)
	tests := []struct {
		millisPtr *int64
		name      string
		millis    int64
		wantZero  bool
	}{
		{name: "zero millis", millis: 0, wantZero: true},
		{name: "with pointer", millisPtr: &v, wantZero: false},
		{name: "nil pointer", millisPtr: nil, wantZero: true},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got time.Time
			switch tt.name {
			case "zero millis":
				got = FromMillis(tt.millis)
			default:
				got = FromMillisPtr(tt.millisPtr)
			}

			if tt.wantZero && !got.IsZero() {
				t.Fatalf("FromMillis helper = %v, want zero", got)
			}
			if !tt.wantZero && got.IsZero() {
				t.Fatal("FromMillis helper = zero, want non-zero")
			}
		})
	}
}

func TestLoadLocationOrUTC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		loc  string
		want string
	}{
		{name: "empty", loc: "   ", want: "UTC"},
		{name: "invalid", loc: "invalid/timezone", want: "UTC"},
		{name: "valid", loc: "Asia/Tokyo", want: "Asia/Tokyo"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := LoadLocationOrUTC(tt.loc)
			if got.String() != tt.want {
				t.Fatalf("LoadLocationOrUTC(%q) = %v, want %s", tt.loc, got, tt.want)
			}
		})
	}
}
