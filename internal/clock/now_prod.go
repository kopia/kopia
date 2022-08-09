//go:build !testing
// +build !testing

package clock

import "time"

// Now returns current wall clock time.
func Now() time.Time {
	return discardMonotonicTime(time.Now()) //nolint:forbidigo
}
