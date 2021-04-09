package timetrack

import (
	"sync/atomic"
	"time"
)

// Throttle throttles UI updates to a specific interval.
type Throttle int64

// ShouldOutput returns true if it's ok to produce output given the for a given time interval.
func (t *Throttle) ShouldOutput(interval time.Duration) bool {
	nextOutputTimeUnixNano := atomic.LoadInt64((*int64)(t))
	if nowNano := time.Now().UnixNano(); nowNano > nextOutputTimeUnixNano { //nolint:forbidigo
		if atomic.CompareAndSwapInt64((*int64)(t), nextOutputTimeUnixNano, nowNano+interval.Nanoseconds()) {
			return true
		}
	}

	return false
}

// Reset resets the throttle.
func (t *Throttle) Reset() {
	*t = 0
}
