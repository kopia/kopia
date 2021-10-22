//go:build !testing
// +build !testing

package clock

import "time"

// WallClockTime returns current wall clock time.
func WallClockTime() time.Time {
	return discardMonotonicTime(time.Now()) // nolint:forbidigo
}
