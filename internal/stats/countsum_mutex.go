//go:build !amd64
// +build !amd64

// Package stats provides helpers for simple stats.  This implementation uses mutex to work around
// ARM alignment issues with CountSum due to unpredictable memory alignment.
package stats

import (
	"sync"
)

// CountSum holds sum and count values.
type CountSum struct {
	mu sync.Mutex
	// +checklocks:mu
	sum int64
	// +checklocks:mu
	count uint32
}

// Add adds size to s and returns approximate values for the current count
// and total bytes.
func (s *CountSum) Add(size int64) (count uint32, sum int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++
	s.sum += size

	return s.count, s.sum
}

// Approximate returns an approximation of the current count and sum values.
// It is approximate because retrieving both values is not an atomic operation.
func (s *CountSum) Approximate() (count uint32, sum int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.count, s.sum
}
