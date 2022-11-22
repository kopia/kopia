package timetrack

import (
	"sync/atomic"
	"time"
)

// Throttle throttles UI updates to a specific interval.
type Throttle struct {
	v atomic.Int64
}

// ShouldOutput returns true if it's ok to produce output given the for a given time interval.
func (t *Throttle) ShouldOutput(interval time.Duration) bool {
	nextOutputTimeUnixNano := t.v.Load()
	if nowNano := time.Now().UnixNano(); nowNano > nextOutputTimeUnixNano { //nolint:forbidigo
		if t.v.CompareAndSwap(nextOutputTimeUnixNano, nowNano+interval.Nanoseconds()) {
			return true
		}
	}

	return false
}

// Reset resets the throttle.
func (t *Throttle) Reset() {
	t.v.Store(0)
}
