//go:build amd64
// +build amd64

// Package stats provides helpers for simple stats
package stats

import "sync/atomic"

// CountSum holds sum and count values.
type CountSum struct {
	// +checkatomic
	sum int64
	// +checkatomic
	count uint32
}

// Add adds size to s and returns approximate values for the current count
// and total bytes.
func (s *CountSum) Add(size int64) (count uint32, sum int64) {
	return atomic.AddUint32(&s.count, 1), atomic.AddInt64(&s.sum, size)
}

// Approximate returns an approximation of the current count and sum values.
// It is approximate because retrieving both values is not an atomic operation.
func (s *CountSum) Approximate() (count uint32, sum int64) {
	return atomic.LoadUint32(&s.count), atomic.LoadInt64(&s.sum)
}
