// Package stats provides helpers for simple stats
package stats

import "sync/atomic"

// CountSum holds sum and count values.
type CountSum struct {
	sum   atomic.Int64
	count atomic.Uint32
}

// Add adds size to s and returns approximate values for the current count
// and total bytes.
func (s *CountSum) Add(size int64) (count uint32, sum int64) {
	return s.count.Add(1), s.sum.Add(size)
}

// Approximate returns an approximation of the current count and sum values.
// It is approximate because retrieving both values is not an atomic operation.
func (s *CountSum) Approximate() (count uint32, sum int64) {
	return s.count.Load(), s.sum.Load()
}
