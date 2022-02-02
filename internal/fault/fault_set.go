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
	mu          sync.Mutex
	faults      map[Method][]*Fault
	callCounter map[Method]int
}

func (s *Set) ensureInitialized() {
	if s.faults == nil {
		s.faults = map[Method][]*Fault{}
		s.callCounter = map[Method]int{}
	}
}

// AddFault adds a new fault for a given method.
func (s *Set) AddFault(method Method) *Fault {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureInitialized()

	f := New()
	s.faults[method] = append(s.faults[method], f)

	return f
}

// AddFaults adds a new fault for a given method.
func (s *Set) AddFaults(method Method, faults ...*Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureInitialized()

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
	s.mu.Lock()

	s.ensureInitialized()

	s.callCounter[method]++

	faults := s.faults[method]
	if len(faults) == 0 {
		s.mu.Unlock()

		return false, nil
	}

	f := faults[0]
	if f.repeatCount > 0 {
		f.repeatCount--
		log(ctx).Debugf("will repeat %v more times the fault for %v %v", f.repeatCount, method, args)
	} else {
		if remaining := faults[1:]; len(remaining) > 0 {
			s.faults[method] = remaining
		} else {
			delete(s.faults, method)
		}
	}

	s.mu.Unlock()

	if f.sleep > 0 {
		log(ctx).Debugf("sleeping for %v in %v %v", f.sleep, method, args)
		time.Sleep(f.sleep)
	}

	if f.callback != nil {
		f.callback()
	}

	if f.errCallback != nil {
		err := f.errCallback()
		log(ctx).Debugf("returning %v for %v %v", err, method, args)

		return true, err
	}

	return false, nil
}
