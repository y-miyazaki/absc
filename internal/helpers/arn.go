// Package helpers provides reusable pure utility functions.
//
//revive:disable:comments-density reason: tiny helper file with self-explanatory functions.
package helpers

import "strings"

// ResourceNameFromARN extracts the trailing resource name from an ARN-like identifier.
func ResourceNameFromARN(arn string) string {
	if arn == "" {
		return ""
	}
	parts := strings.Split(arn, "/")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}
	parts = strings.Split(arn, ":")
	return parts[len(parts)-1]
}
