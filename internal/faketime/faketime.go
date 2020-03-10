// Package faketime fakes time for tests
package faketime

import (
	"sync"
	"sync/atomic"
	"time"
)

// Frozen returns a function that always returns t
func Frozen(t time.Time) func() time.Time {
	return func() time.Time {
		return t
	}
}

// AutoAdvance returns a time source function that returns a time equal to
// 't + ((n - 1) * dt)' wheren n is the number of serialized invocations of
// the returned function. The returned function will generate a time series of
// the form [t, t+dt, t+2dt, t+3dt, ...]
func AutoAdvance(t time.Time, dt time.Duration) func() time.Time {
	var mu sync.Mutex

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		ret := t
		t = t.Add(dt)

		return ret
	}
}

// TimeAdvance allows controlling the passage of time. Intended to be used in
// tests.
type TimeAdvance struct {
	base  time.Time
	delta int64
}

// NewTimeAdvance creates a TimeAdvance with the given start time
func NewTimeAdvance(start time.Time) *TimeAdvance {
	return &TimeAdvance{base: start}
}

// NowFunc returns a time provider function for t
func (t *TimeAdvance) NowFunc() func() time.Time {
	return func() time.Time {
		dt := atomic.LoadInt64(&t.delta)

		return t.base.Add(time.Duration(dt))
	}
}

// Advance advances t by dt, such that the next call to t.NowFunc()() returns
// current t + dt
func (t *TimeAdvance) Advance(dt time.Duration) time.Time {
	advance := atomic.AddInt64(&t.delta, int64(dt))

	return t.base.Add(time.Duration(advance))
}
