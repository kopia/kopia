// Package faketime fakes time for tests
package faketime

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

// Frozen returns a function that always returns t.
func Frozen(t time.Time) func() time.Time {
	return func() time.Time {
		return t
	}
}

// AutoAdvance returns a time source function that returns a time equal to
// 'start + ((n - 1) * dt)' wheren n is the number of serialized invocations of
// the returned function. The returned function will generate a time series of
// the form [start, start+dt, start+2dt, start+3dt, ...].
func AutoAdvance(start time.Time, dt time.Duration) func() time.Time {
	return NewAutoAdvance(start, dt).NowFunc()
}

// TimeAdvance allows controlling the passage of time. Intended to be used in
// tests.
type TimeAdvance struct {
	delta  atomic.Int64
	autoDt int64
	base   time.Time
}

// NewTimeAdvance creates a TimeAdvance clock with the given start time.
// The returned clock does not automatically advance time when NowFunc is called.
func NewTimeAdvance(start time.Time) *TimeAdvance {
	return NewAutoAdvance(start, 0)
}

// NewAutoAdvance creates an auto-advancing clock with the given start time and
// autoDelta automatic time increase en each call to NowFunc().
func NewAutoAdvance(start time.Time, autoDelta time.Duration) *TimeAdvance {
	return &TimeAdvance{
		autoDt: int64(autoDelta),
		base:   start,
	}
}

// NowFunc returns a time provider function for t.
func (t *TimeAdvance) NowFunc() func() time.Time {
	return func() time.Time {
		dt := t.delta.Add(t.autoDt) - t.autoDt

		return t.base.Add(time.Duration(dt))
	}
}

// Advance advances t by dt, such that the next call to t.NowFunc()() returns
// current t + dt.
func (t *TimeAdvance) Advance(dt time.Duration) time.Time {
	advance := t.delta.Add(int64(dt))

	return t.base.Add(time.Duration(advance))
}

// ClockTimeWithOffset allows controlling the passage of time. Intended to be used in
// tests.
type ClockTimeWithOffset struct {
	mu sync.Mutex
	// +checklocks:mu
	offset time.Duration
}

// NewClockTimeWithOffset creates a ClockTimeWithOffset with the given start time.
func NewClockTimeWithOffset(offset time.Duration) *ClockTimeWithOffset {
	return &ClockTimeWithOffset{offset: offset}
}

// NowFunc returns a time provider function for t.
func (t *ClockTimeWithOffset) NowFunc() func() time.Time {
	return func() time.Time {
		t.mu.Lock()
		defer t.mu.Unlock()

		return clock.Now().Add(t.offset)
	}
}

// Advance increases the time offset by dt, such that the next call to
// t.NowFunc()() returns current time + new offset.
func (t *ClockTimeWithOffset) Advance(dt time.Duration) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.offset += dt

	return clock.Now().Add(t.offset)
}
