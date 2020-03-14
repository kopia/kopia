// Package buf manages allocation of temporary short-term buffers.
package buf

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxAllocAttempts       = 3
	allocationRetryTimeout = 500 * time.Millisecond
)

type segment struct {
	nextUnallocated   int32  // high water mark
	allocatedBufCount int32  // how many outstanding users of the segment there are
	data              []byte // the underlying buffer from which we're allocating
	pool              *Pool
}

// Buf represents allocated slice of memory pool. At the end of using the buffer, Release() must be called to
// reclaim memory.
type Buf struct {
	nextUnallocated         int32
	previousNextUnallocated int32

	Data []byte

	segment *segment // segment from which the data was allocated
}

// IsPooled determines whether data slice is part of a pool.
func (b *Buf) IsPooled() bool { return b.segment != nil }

// Release returns the slice back to the pool.
func (b *Buf) Release() {
	if b.segment == nil {
		return
	}

	notify := false

	// best effort compare-and-swap, which will pop the buffer off the stack in its appropriate segment
	if atomic.CompareAndSwapInt32(&b.segment.nextUnallocated, b.nextUnallocated, b.previousNextUnallocated) {
		// popped from the stack in the segment
		notify = true
	}

	if atomic.AddInt32(&b.segment.allocatedBufCount, -1) == 0 {
		// last allocated Buf, we can reset 'next' to zero
		atomic.StoreInt32(&b.segment.nextUnallocated, 0)

		notify = true
	}

	if notify {
		// this segment just became free, notify other goroutines doing Allocate()
		select {
		case b.segment.pool.segmentReleased <- struct{}{}: // notified
		default: // nobody is waiting
		}
	}

	b.Data = nil

	b.segment = nil
}

func (s *segment) allocate(count int) (Buf, bool) {
	n := int32(count)

	for {
		// see if we have capacity in this segment
		v := atomic.LoadInt32(&s.nextUnallocated)
		if v+n > int32(len(s.data)) {
			// out of space in this segment
			return Buf{}, false
		}

		if atomic.CompareAndSwapInt32(&s.nextUnallocated, v, v+n) {
			atomic.AddInt32(&s.allocatedBufCount, 1)

			return Buf{v + n, v, s.data[v : v+n : v+n], s}, true
		}
	}
}

// Pool manages allocations of short-term data buffers from a pool.
//
// Note that buffers managed by the pool are meant to be extremely short lived and are suitable
// for in-memory operations, such as encryption, compression, etc, but not for I/O buffers of any kind.
// It is EXTREMELY important to always release memory allocated from the Pool. Failure to do so will
// result in
//
// The pool uses N segments, with each segment tracking its high water mark usage.
//
// Allocation simply advances the high water mark within first segment that has capacity
// and increments per-segment refcount.
//
// On Buf.Release() the refcount is decremented and when it hits zero, the entire segment becomes instantly
// freed.
//
// As an extra optimization, when Buf.Release() is called in LIFO order, it will also lower the
// high water mark making its memory available for immediate reuse.
//
// If no segment has available capacity, the pool waits a few times until memory becomes released
// and falls back to allocating from the heap.
type Pool struct {
	maxSegments     int
	segmentReleased chan struct{} // channel which is notified whenever any segment becomes available for allocations

	// this protects the slice, to be able to atomically replace it
	mu          sync.Mutex
	segmentSize int
	segments    []*segment
}

// NewPool creates a buffer pool, composed of N fixed-length segments of specified maximum size.
func NewPool(segmentSize, maxSegments int) *Pool {
	p := &Pool{
		segmentSize:     segmentSize,
		segmentReleased: make(chan struct{}),
		maxSegments:     maxSegments,
	}

	return p
}

func (p *Pool) currentSegments() []*segment {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.segments
}

// SetSegmentSize sets the segment size for future segments that will be created.
func (p *Pool) SetSegmentSize(maxSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.segmentSize = maxSize
}

// InitializeSegments initializes n segments up to the maximum number of segments.
func (p *Pool) InitializeSegments(n int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	remaining := p.maxSegments - len(p.segments)
	if n > remaining {
		n = remaining
	}

	if n == 0 {
		return false
	}

	var newSegments []*segment

	newSegments = append(newSegments, p.segments...)

	for i := 0; i < n; i++ {
		newSegments = append(newSegments, &segment{
			data: make([]byte, p.segmentSize),
			pool: p,
		})
	}

	p.segments = newSegments

	return true
}

// Allocate allocates a slice of the buffer
func (p *Pool) Allocate(n int) Buf {
	// requested more than the pool can cache, allocate throw-away buffer.
	if p == nil || n > p.segmentSize {
		return Buf{0, 0, make([]byte, n), nil}
	}

	for i := 0; i < maxAllocAttempts; i++ {
		// try to allocate
		for _, s := range p.currentSegments() {
			buf, ok := s.allocate(n)
			if ok {
				return buf
			}
		}

		// add one more segment up to specified limit
		if p.InitializeSegments(1) {
			continue
		}

		// wait until some segment becomes free or some time passes, whichever comes first
		select {
		case <-p.segmentReleased:
		case <-time.After(allocationRetryTimeout):
		}
	}

	// fall back to heap allocation
	return Buf{0, 0, make([]byte, n), nil}
}
