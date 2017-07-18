package repo

import (
	"fmt"
	"strings"
	"sync"
)

type writebackManager struct {
	workers   int
	semaphore semaphore
	errors    asyncErrors
	waitGroup sync.WaitGroup
}

func (w *writebackManager) enabled() bool {
	return w.workers > 0
}

func (w *writebackManager) flush() {
	if w.workers > 0 {
		w.waitGroup.Wait()
	}
}

type asyncErrors struct {
	sync.RWMutex
	errors []error
}

func (e *asyncErrors) add(err error) {
	e.Lock()
	e.errors = append(e.errors, err)
	e.Unlock()
}

func (e *asyncErrors) check() error {
	e.RLock()
	defer e.RUnlock()

	switch len(e.errors) {
	case 0:
		return nil
	case 1:
		return e.errors[0]
	default:
		msg := make([]string, len(e.errors))
		for i, err := range e.errors {
			msg[i] = err.Error()
		}

		return fmt.Errorf("%v errors: %v", len(e.errors), strings.Join(msg, ";"))
	}
}
