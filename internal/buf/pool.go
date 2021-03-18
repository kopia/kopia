// Package buf manages allocation of temporary short-term buffers.
package buf

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type segment struct {
	mu sync.RWMutex

	nextUnallocated   int    // high water mark
	allocatedBufCount int    // how many outstanding users of the segment there are
	data              []byte // the underlying buffer from which we're allocating
	pool              *Pool
}

// Buf represents allocated slice of memory pool. At the end of using the buffer, Release() must be called to
// reclaim memory.
type Buf struct {
	Data []byte

	nextUnallocated         int
	previousNextUnallocated int
	segment                 *segment // segment from which the data was allocated
}

// IsPooled determines whether data slice is part of a pool.
func (b *Buf) IsPooled() bool { return b.segment != nil }

// Release returns the slice back to the pool.
func (b *Buf) Release() {
	if b.segment == nil {
		return
	}

	atomic.AddInt64(&b.segment.pool.totalReleasedBytes, int64(len(b.Data)))
	atomic.AddInt64(&b.segment.pool.totalReleasedBuffers, 1)

	b.segment.mu.Lock()
	defer b.segment.mu.Unlock()

	// best effort compare-and-swap, which will pop the buffer off the stack in its appropriate segment
	if b.segment.nextUnallocated == b.nextUnallocated {
		b.segment.nextUnallocated = b.previousNextUnallocated
	}

	b.segment.allocatedBufCount--
	if b.segment.allocatedBufCount == 0 {
		// last allocated Buf, we can reset 'next' to zero
		b.segment.nextUnallocated = 0
	}

	b.Data = nil
	b.segment = nil
}

func (s *segment) allocate(n int) (Buf, bool) {
	// quick check using shared lock
	s.mu.RLock()
	haveRoom := s.nextUnallocated+n <= len(s.data)
	s.mu.RUnlock()

	if !haveRoom {
		return Buf{}, false
	}

	// we likely have space, allocate under exclusive lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// see if we have capacity in this segment
	nu := s.nextUnallocated

	if nu+n > len(s.data) {
		// out of space in this segment
		return Buf{}, false
	}

	s.allocatedBufCount++
	s.nextUnallocated += n

	return Buf{
		Data:                    s.data[nu : nu+n : nu+n],
		nextUnallocated:         nu + n,
		previousNextUnallocated: nu,
		segment:                 s,
	}, true
}

// Pool manages allocations of short-term data buffers from a pool.
//
// Note that buffers managed by the pool are meant to be extremely short lived and are suitable
// for in-memory operations, such as encryption, compression, etc, but not for I/O buffers of any kind.
// It is EXTREMELY important to always release memory allocated from the Pool. Failure to do so will
// result in memory leaks.
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
	totalAllocatedBytes   int64
	totalReleasedBytes    int64
	totalAllocatedBuffers int64
	totalReleasedBuffers  int64

	poolID string

	closed chan struct{}

	tagMutators []tag.Mutator

	// this protects the slice, to be able to atomically replace it
	mu          sync.Mutex
	segmentSize int
	segments    []*segment
}

var (
	activePoolsMutex sync.Mutex
	activePools      = map[*Pool]string{}
)

// NewPool creates a buffer pool, composed of fixed-length segments of specified maximum size.
func NewPool(ctx context.Context, segmentSize int, poolID string) *Pool {
	p := &Pool{
		poolID:      poolID,
		tagMutators: []tag.Mutator{tag.Insert(tagKeyPool, poolID)},
		segmentSize: segmentSize,
		closed:      make(chan struct{}),
	}

	activePoolsMutex.Lock()
	activePools[p] = string(debug.Stack())
	activePoolsMutex.Unlock()

	go func() {
		for {
			select {
			case <-p.closed:
				return

			case <-time.After(1 * time.Second):
				p.reportMetrics(ctx)
			}
		}
	}()

	return p
}

// Close closes the pool.
func (p *Pool) Close() {
	close(p.closed)

	activePoolsMutex.Lock()
	delete(activePools, p)
	activePoolsMutex.Unlock()
}

// ActivePools returns the set of active activePools.
func ActivePools() map[*Pool]string {
	activePoolsMutex.Lock()
	defer activePoolsMutex.Unlock()

	r := map[*Pool]string{}
	for k, v := range activePools {
		r[k] = v
	}

	return r
}

func (p *Pool) reportMetrics(ctx context.Context) {
	allBytes := atomic.LoadInt64(&p.totalAllocatedBytes)
	relBytes := atomic.LoadInt64(&p.totalReleasedBytes)
	allBuffers := atomic.LoadInt64(&p.totalAllocatedBuffers)
	relBuffers := atomic.LoadInt64(&p.totalReleasedBuffers)

	_ = stats.RecordWithTags(
		ctx,
		p.tagMutators,
		metricPoolAllocatedBytes.M(allBytes),
		metricPoolReleasedBytes.M(relBytes),
		metricPoolOutstandingBytes.M(allBytes-relBytes),
		metricPoolAllocatedBuffers.M(allBuffers),
		metricPoolReleasedBuffers.M(relBuffers),
		metricPoolOutstandingBuffers.M(allBuffers-relBuffers),
		metricPoolNumSegments.M(int64(len(p.currentSegments()))),
	)
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

// AddSegments n segments to the pool.
func (p *Pool) AddSegments(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var newSegments []*segment

	newSegments = append(newSegments, p.segments...)

	for i := 0; i < n; i++ {
		newSegments = append(newSegments, &segment{
			data: make([]byte, p.segmentSize),
			pool: p,
		})
	}

	p.segments = newSegments
}

// Allocate allocates from the buffer a slice of size n.
func (p *Pool) Allocate(n int) Buf {
	// requested more than the pool can cache, allocate throw-away buffer.
	if p == nil || n > p.segmentSize {
		return Buf{make([]byte, n), 0, 0, nil}
	}

	atomic.AddInt64(&p.totalAllocatedBytes, int64(n))
	atomic.AddInt64(&p.totalAllocatedBuffers, 1)

	for {
		// try to allocate
		for _, s := range p.currentSegments() {
			buf, ok := s.allocate(n)
			if ok {
				return buf
			}
		}

		// add one more segment
		p.AddSegments(1)
	}
}
