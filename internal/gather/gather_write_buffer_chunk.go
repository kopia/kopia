package gather

import (
	"context"
	"sync"

	"github.com/alecthomas/units"
)

var (
	defaultAllocator = &chunkAllocator{
		name:            "default",
		chunkSize:       1 << 16, // nolint:gomnd
		maxFreeListSize: 512,     // nolint:gomnd
	}

	contiguousAllocator = &chunkAllocator{
		name:            "contiguous",
		chunkSize:       8<<20 + 128, // nolint:gomnd
		maxFreeListSize: 2,           // nolint:gomnd
	}
)

type chunkAllocator struct {
	name      string
	chunkSize int

	mu                    sync.Mutex
	freeList              [][]byte
	maxFreeListSize       int
	freeListHighWaterMark int
	allocHighWaterMark    int
	allocated             int
	freed                 int
}

func (a *chunkAllocator) allocChunk() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.allocated++

	if tot := a.allocated - a.freed; tot > a.allocHighWaterMark {
		a.allocHighWaterMark = tot
	}

	l := len(a.freeList)
	if l == 0 {
		return make([]byte, 0, a.chunkSize)
	}

	ch := a.freeList[l-1]
	a.freeList = a.freeList[0 : l-1]

	return ch
}

func (a *chunkAllocator) releaseChunk(s []byte) {
	if cap(s) != a.chunkSize {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.freed++

	if len(a.freeList) < a.maxFreeListSize {
		a.freeList = append(a.freeList, s[:0])
	}

	if len(a.freeList) > a.freeListHighWaterMark {
		a.freeListHighWaterMark = len(a.freeList)
	}
}

func (a *chunkAllocator) dumpStats(ctx context.Context, prefix string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	log(ctx).Infof("%v (%v) - allocated %v chunks freed %v alive %v max %v free list high water mark: %v",
		prefix,
		units.Base2Bytes(int64(a.chunkSize)),
		a.allocated, a.freed, a.allocated-a.freed, a.allocHighWaterMark, a.freeListHighWaterMark)
}

// DumpStats logs the allocator statistics.
func DumpStats(ctx context.Context) {
	defaultAllocator.dumpStats(ctx, "default")
	contiguousAllocator.dumpStats(ctx, "contig")
}
