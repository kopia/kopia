// Package faketime fakes time for tests
package faketime

import (
	"sync"
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
