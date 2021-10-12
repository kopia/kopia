package gather

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/alecthomas/units"
)

const (
	mynamespace                  = "kopia/kopia/internal/gather"
	maxCallersToTrackAllocations = 3
)

var (
	trackChunkAllocations = os.Getenv("KOPIA_TRACK_CHUNK_ALLOC") != ""

	defaultAllocator = &chunkAllocator{
		name:            "default",
		chunkSize:       1 << 16, // nolint:gomnd
		maxFreeListSize: 2048,    // nolint:gomnd
	}

	// typicalContiguousAllocator is used for short-term buffers for encryption.
	typicalContiguousAllocator = &chunkAllocator{
		name:            "mid-size contiguous",
		chunkSize:       8<<20 + 128, // nolint:gomnd
		maxFreeListSize: runtime.NumCPU(),
	}

	// maxContiguousAllocator is used for short-term buffers for encryption.
	maxContiguousAllocator = &chunkAllocator{
		name:            "contiguous",
		chunkSize:       16<<20 + 128, // nolint:gomnd
		maxFreeListSize: runtime.NumCPU(),
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
	slicesAllocated       int
	freed                 int
	activeChunks          map[uintptr]string
}

func (a *chunkAllocator) trackAlloc(v []byte) []byte {
	if trackChunkAllocations {
		var (
			pcbuf        [8]uintptr
			callerFrames []string
		)

		n := runtime.Callers(maxCallersToTrackAllocations, pcbuf[:])
		frames := runtime.CallersFrames(pcbuf[0:n])

		for f, ok := frames.Next(); ok; f, ok = frames.Next() {
			fn := fmt.Sprintf("%v %v:%v", f.Func.Name(), f.File, f.Line)

			if fn != "" && !strings.Contains(fn, mynamespace) {
				callerFrames = append(callerFrames, fn)
			}
		}

		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&v)) //nolint:gosec

		if a.activeChunks == nil {
			a.activeChunks = map[uintptr]string{}
		}

		a.activeChunks[hdr.Data] = strings.Join(callerFrames, "\n")
	}

	return v
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
		a.slicesAllocated++
		return a.trackAlloc(make([]byte, 0, a.chunkSize))
	}

	ch := a.freeList[l-1]
	a.freeList = a.freeList[0 : l-1]

	return a.trackAlloc(ch)
}

func (a *chunkAllocator) releaseChunk(s []byte) {
	if cap(s) != a.chunkSize {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.activeChunks != nil {
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s)) //nolint:gosec
		delete(a.activeChunks, hdr.Data)
	}

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

	alive := a.allocated - a.freed

	log(ctx).Debugf("%v (%v) - allocated %v(%v) chunks freed %v alive %v max %v free list high water mark: %v",
		prefix,
		units.Base2Bytes(int64(a.chunkSize)),
		a.allocated, a.slicesAllocated, a.freed, alive, a.allocHighWaterMark, a.freeListHighWaterMark)

	for _, v := range a.activeChunks {
		log(ctx).Debugf("leaked chunk from %v", v)
	}

	if trackChunkAllocations && len(a.activeChunks) > 0 {
		// nolint:gocritic
		os.Exit(1)
	}
}

// DumpStats logs the allocator statistics.
func DumpStats(ctx context.Context) {
	defaultAllocator.dumpStats(ctx, "default")
	typicalContiguousAllocator.dumpStats(ctx, "typical-contig")
	maxContiguousAllocator.dumpStats(ctx, "contig")
}
