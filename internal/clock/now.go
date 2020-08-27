// Package clock provides indirection for accessing current time.
package clock

import (
	"time"
)

// Since returns time since the given timestamp.
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// Until returns duration of time between now and a given time.
func Until(t time.Time) time.Duration {
	return t.Sub(Now())
}
