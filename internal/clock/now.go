// Package clock provides indirection for accessing current wall clock time.
// this is suitable for timestamps and long-term time operations, not short-term time measurements.
package clock

import (
	"time"
)

// discardMonotonicTime discards any monotonic time component of time,
// which behaves incorrectly when the computer goes to sleep and we want to measure durations
// between points in time.
//
// See https://go.googlesource.com/proposal/+/master/design/12914-monotonic.md
func discardMonotonicTime(t time.Time) time.Time {
	return time.Unix(0, t.UnixNano())
}
