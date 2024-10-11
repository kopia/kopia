package throttling

import (
	"sync"

	"github.com/pkg/errors"
)

type semaphore struct {
	mu sync.Mutex
	// +checklocks:mu
	sem chan struct{}
}

func (s *semaphore) getChan() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sem
}

func (s *semaphore) Acquire() {
	ch := s.getChan()
	if ch == nil {
		return
	}

	// push to channel, may block
	ch <- struct{}{}
}

func (s *semaphore) Release() {
	ch := s.getChan()
	if ch == nil {
		return
	}

	select {
	case <-ch: // removed one entry from channel
	default: // this can happen when we reset a channel to a lower value
	}
}

func (s *semaphore) SetLimit(limit int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit < 0 {
		return errors.New("invalid limit")
	}

	if limit > 0 {
		s.sem = make(chan struct{}, limit)
	} else {
		s.sem = nil
	}

	return nil
}

func newSemaphore() *semaphore {
	return &semaphore{}
}
