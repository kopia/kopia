package fault

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("fault")

// Method indicates a method with fault injection.
type Method int

// Set encapsulates a set of faults.
type Set struct {
	mu sync.Locker
	// +checklocks:mu
	faults map[Method][]*Fault
	// +checklocks:mu
	callCounter map[Method]int
}

func NewSet() *Set {
	q := &Set{
		mu:          &sync.Mutex{},
		faults:      map[Method][]*Fault{},
		callCounter: map[Method]int{},
	}

	return q
}

// AddFault adds a new fault for a given method.
func (s *Set) AddFault(method Method) *Fault {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := New()
	s.faults[method] = append(s.faults[method], f)

	return f
}

// AddFaults adds a new fault for a given method.
func (s *Set) AddFaults(method Method, faults ...*Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.faults[method] = append(s.faults[method], faults...)
}

// NumCalls returns the number of calls for a particular method.
func (s *Set) NumCalls(method Method) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.callCounter[method]
}

// VerifyAllFaultsExercised fails the test if some faults have not been exercised.
func (s *Set) VerifyAllFaultsExercised(t *testing.T) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.faults) != 0 {
		t.Fatalf("not all defined faults have been hit: %#v", s.faults)
	}
}

// GetNextFault returns the error message to return on next fault.
func (s *Set) GetNextFault(ctx context.Context, method Method, args ...interface{}) (bool, error) {
	// Lock set for map accesses.  Call counters will be updated for the fault-set, and the fault for the fault-set method
	// will be gotten.
	s.mu.Lock()

	s.callCounter[method]++

	faults := s.faults[method]
	if len(faults) == 0 {
		s.mu.Unlock()

		return false, nil
	}

	// Access the "next" fault.  The fault at the end of the queue
	f := faults[0]
	// `fault` comes from `s.faults` so nested locks held at this point.
	f.mu.Lock()

	// Count down repeat count in fault.
	if f.repeatCount > 0 {
		f.repeatCount--
		log(ctx).Debugf("will repeat %v more times the fault for %v %v", f.repeatCount, method, args)
	} else {
		// `repeatCount` == 0.  Remove the fault if there are faults remaining in the queue ...
		if remaining := faults[1:]; len(remaining) > 0 {
			s.faults[method] = remaining
		} else {
			// ... otherwise delete the map entry for the method
			delete(s.faults, method)
		}
	}

	delay := f.sleep

	// Two locks are held, so unlock both before waiting.
	f.mu.Unlock()

	s.mu.Unlock()

	if delay > 0 {
		// sleep for a while
		log(ctx).Debugf("sleeping for %v in %v %v", delay, method, args)
		time.Sleep(delay)
	}

	// Re-acquire fault lock to get callback functions.  Callbacks will be called without lock.
	f.mu.Lock()

	cb := f.callback
	errCb := f.errCallback

	f.mu.Unlock()

	// No more references to `f`.  Perform callbacks inline.
	if cb != nil {
		cb()
	}

	if errCb != nil {
		err := errCb()
		log(ctx).Debugf("returning %v for %v %v", err, method, args)

		return true, err
	}

	return false, nil
}
