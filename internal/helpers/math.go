// Package helpers provides reusable pure utility functions.
//
//revive:disable:comments-density reason: tiny helper file with self-explanatory functions.
package helpers

const maxInt32Value = 1<<31 - 1

// SafeInt32 clamps an int into the non-negative int32 range.
func SafeInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > maxInt32Value {
		return maxInt32Value
	}
	return int32(value)
}
