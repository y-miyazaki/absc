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

	for _, tt := range tests {
		tt := tt
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

	zero := FormatRFC3339NanoUTC(time.Time{})
	if zero != "" {
		t.Fatalf("FormatRFC3339NanoUTC(zero) = %q, want empty", zero)
	}

	got := FormatRFC3339NanoUTC(time.Date(2026, time.April, 20, 1, 2, 3, 456, time.FixedZone("JST", 9*3600)))
	if got != "2026-04-19T16:02:03.000000456Z" {
		t.Fatalf("FormatRFC3339NanoUTC() = %q", got)
	}
}

func TestFormatRFC3339UTC(t *testing.T) {
	t.Parallel()

	zero := FormatRFC3339UTC(time.Time{})
	if zero != "" {
		t.Fatalf("FormatRFC3339UTC(zero) = %q, want empty", zero)
	}

	got := FormatRFC3339UTC(time.Date(2026, time.April, 20, 9, 0, 0, 0, time.FixedZone("JST", 9*3600)))
	if got != "2026-04-20T00:00:00Z" {
		t.Fatalf("FormatRFC3339UTC() = %q", got)
	}
}

func TestFormatUTCOffset(t *testing.T) {
	t.Parallel()

	if got := FormatUTCOffset(9 * 3600); got != "UTC+09:00" {
		t.Fatalf("FormatUTCOffset(32400) = %q", got)
	}
	if got := FormatUTCOffset(-(5*3600 + 30*60)); got != "UTC-05:30" {
		t.Fatalf("FormatUTCOffset(-19800) = %q", got)
	}
}

func TestFromMillisAndPtr(t *testing.T) {
	t.Parallel()

	if got := FromMillis(0); !got.IsZero() {
		t.Fatalf("FromMillis(0) = %v, want zero", got)
	}

	v := int64(1713571200000)
	fromPtr := FromMillisPtr(&v)
	fromValue := FromMillis(v)
	if !fromPtr.Equal(fromValue) {
		t.Fatalf("FromMillisPtr(%d) = %v, want %v", v, fromPtr, fromValue)
	}

	if got := FromMillisPtr(nil); !got.IsZero() {
		t.Fatalf("FromMillisPtr(nil) = %v, want zero", got)
	}
}

func TestLoadLocationOrUTC(t *testing.T) {
	t.Parallel()

	if got := LoadLocationOrUTC("   "); got != time.UTC {
		t.Fatalf("LoadLocationOrUTC(empty) = %v, want UTC", got)
	}
	if got := LoadLocationOrUTC("invalid/timezone"); got != time.UTC {
		t.Fatalf("LoadLocationOrUTC(invalid) = %v, want UTC", got)
	}
	if got := LoadLocationOrUTC("Asia/Tokyo"); got.String() != "Asia/Tokyo" {
		t.Fatalf("LoadLocationOrUTC(Asia/Tokyo) = %v", got)
	}
}
