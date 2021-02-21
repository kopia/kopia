// +build !testing

package clock

import "time"

// Now returns current wall clock time.
func Now() time.Time {
	return time.Now() // nolint:forbidigo
}
