// Package helpers provides reusable pure utility functions.
//
//revive:disable:comments-density reason: helper functions are straightforward and intentionally compact.
package helpers

import (
	"fmt"
	"strings"
	"time"
)

const (
	secondsPerHour   = 3600
	secondsPerMinute = 60
)

// ConvertRFC3339ToLocation converts an RFC3339 timestamp string into the given location.
func ConvertRFC3339ToLocation(value string, loc *time.Location) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return value
	}
	return t.In(loc).Format(time.RFC3339)
}

// FormatRFC3339NanoUTC formats a time in UTC using RFC3339Nano, or returns an empty string for zero values.
func FormatRFC3339NanoUTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// FormatRFC3339UTC formats a time in UTC using RFC3339, or returns an empty string for zero values.
func FormatRFC3339UTC(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// FormatUTCOffset renders a UTC offset in +HH:MM form.
func FormatUTCOffset(offsetSeconds int) string {
	sign := "+"
	absOffsetSeconds := offsetSeconds
	if absOffsetSeconds < 0 {
		sign = "-"
		absOffsetSeconds = -absOffsetSeconds
	}
	hours := absOffsetSeconds / secondsPerHour
	minutes := (absOffsetSeconds % secondsPerHour) / secondsPerMinute
	return fmt.Sprintf("UTC%s%02d:%02d", sign, hours, minutes)
}

// FromMillis converts a Unix millisecond timestamp into a UTC time.
func FromMillis(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

// FromMillisPtr converts an optional Unix millisecond timestamp into a UTC time.
func FromMillisPtr(v *int64) time.Time {
	if v == nil {
		return time.Time{}
	}
	return FromMillis(*v)
}

// LoadLocationOrUTC loads a timezone and falls back to UTC when it cannot be resolved.
func LoadLocationOrUTC(timezone string) *time.Location {
	tz := strings.TrimSpace(timezone)
	if tz == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.UTC
	}
	return loc
}
