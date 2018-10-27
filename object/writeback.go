package object

import (
	"fmt"
	"strings"
	"sync"
)

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
