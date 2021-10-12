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

// LocalClock is a structure that implements zap.Clock that returns local clock timestamps.
type LocalClock struct{}

// Now implements zap.Clock.
func (c LocalClock) Now() time.Time { return Now().Local() }

// NewTicker implements zap.Clock.
func (c LocalClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// UTCClock is a structure that implements zap.Clock which returns UTC timestamps.
type UTCClock struct{}

// Now implements zap.Clock.
func (c UTCClock) Now() time.Time { return Now().UTC() }

// NewTicker implements zap.Clock.
func (c UTCClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// supported clock instances.
var (
	Local = LocalClock{}
	UTC   = UTCClock{}
)
