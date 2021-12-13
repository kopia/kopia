// Package fault defines fault behaviors.
package fault

import "time"

// Fault describes the behavior of a single fault.
type Fault struct {
	repeatCount int           // how many times to repeat this fault
	sleep       time.Duration // sleep before returning
	callback    func()
	errCallback func() error
}

// New creates a new fault.
func New() *Fault {
	return &Fault{}
}

// ErrorInstead causes the fault to return the provided error instead of calling the method.
func (f *Fault) ErrorInstead(err error) *Fault {
	f.errCallback = func() error { return err }
	return f
}

// ErrorCallbackInstead invokes the provided function to return the error instead of calling the method.
func (f *Fault) ErrorCallbackInstead(cb func() error) *Fault {
	f.errCallback = cb
	return f
}

// Before invokes the provided function but does not return an error.
func (f *Fault) Before(cb func()) *Fault {
	f.callback = cb
	return f
}

// Repeat causes the fault to repeat N times.
func (f *Fault) Repeat(n int) *Fault {
	f.repeatCount = n
	return f
}

// SleepFor sleeps for the specified amount of time.
func (f *Fault) SleepFor(d time.Duration) *Fault {
	f.sleep = d
	return f
}
