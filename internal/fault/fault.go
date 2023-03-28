// Package fault defines fault behaviors.
package fault

import (
	"sync"
	"time"
)

// Fault describes the behavior of a single fault.
type Fault struct {
	// how many times to repeat this fault
	repeatCount int // +checklocks:mu
	// sleep before returning
	sleep       time.Duration // +checklocks:mu
	callback    func() // +checklocks:mu
	errCallback func() error // +checklocks:mu
	mu          sync.Mutex
}

// New creates a new fault.
func New() *Fault {
	return &Fault{}
}

// ErrorInstead causes the fault to return the provided error instead of calling the method.
func (f *Fault) ErrorInstead(err error) *Fault {
	f.mu.Lock()
	f.errCallback = func() error { return err }
	f.mu.Unlock()

	return f
}

// ErrorCallbackInstead invokes the provided function to return the error instead of calling the method.
func (f *Fault) ErrorCallbackInstead(cb func() error) *Fault {
	f.mu.Lock()
	f.errCallback = cb
	f.mu.Unlock()

	return f
}

// Before invokes the provided function but does not return an error.
func (f *Fault) Before(cb func()) *Fault {
	f.mu.Lock()
	f.callback = cb
	f.mu.Unlock()

	return f
}

// Repeat causes the fault to repeat N times.
func (f *Fault) Repeat(n int) *Fault {
	f.mu.Lock()
	f.repeatCount = n
	f.mu.Unlock()

	return f
}

// SleepFor sleeps for the specified amount of time.
func (f *Fault) SleepFor(d time.Duration) *Fault {
	f.mu.Lock()
	f.sleep = d
	f.mu.Unlock()

	return f
}
