package gather

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

const (
	mynamespace                  = "kopia/kopia/internal/gather"
	maxCallersToTrackAllocations = 3
)

//nolint:gochecknoglobals
var (
	trackChunkAllocations = os.Getenv("KOPIA_TRACK_CHUNK_ALLOC") != ""

	defaultAllocator = &chunkAllocator{
		name:            "default",
		chunkSize:       1 << 16, //nolint:mnd
		maxFreeListSize: 2048,    //nolint:mnd
	}

	// typicalContiguousAllocator is used for short-term buffers for encryption.
	typicalContiguousAllocator = &chunkAllocator{
		name:            "mid-size contiguous",
		chunkSize:       8<<20 + 128, //nolint:mnd
		maxFreeListSize: runtime.NumCPU(),
	}

	// maxContiguousAllocator is used for short-term buffers for encryption.
	maxContiguousAllocator = &chunkAllocator{
		name:            "contiguous",
		chunkSize:       16<<20 + 128, //nolint:mnd
		maxFreeListSize: runtime.NumCPU(),
	}
)

type chunkAllocator struct {
	name      string
	chunkSize int

	mu sync.Mutex
	// +checklocks:mu
	freeList [][]byte
	// +checklocks:mu
	maxFreeListSize int
	// +checklocks:mu
	freeListHighWaterMark int
	// +checklocks:mu
	allocHighWaterMark int
	// +checklocks:mu
	allocated int
	// +checklocks:mu
	slicesAllocated int

	// +checklocks:mu
	freed int

	// +checklocks:mu
	activeChunks map[uintptr]string
}

// +checklocks:a.mu
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

		ptr := uintptr(unsafe.Pointer(unsafe.SliceData(v))) //nolint:gosec

		if a.activeChunks == nil {
			a.activeChunks = map[uintptr]string{}
		}

		a.activeChunks[ptr] = strings.Join(callerFrames, "\n")
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
		ptr := uintptr(unsafe.Pointer(unsafe.SliceData(s))) //nolint:gosec
		delete(a.activeChunks, ptr)
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

	log(ctx).Debugw("allocator stats",
		"allocator", prefix,
		"chunkSize", int64(a.chunkSize),

		"chunksAlloc", a.allocated,
		"chunksFreed", a.freed,
		"chunksAlive", alive,

		"allocHighWaterMark", a.allocHighWaterMark,
		"freeListHighWaterMark", a.freeListHighWaterMark,

		"slicesAlloc", a.slicesAllocated,
	)

	for _, v := range a.activeChunks {
		log(ctx).Debugf("leaked chunk from %v", v)
	}
}

// DumpStats logs the allocator statistics.
func DumpStats(ctx context.Context) {
	defaultAllocator.dumpStats(ctx, "default")
	typicalContiguousAllocator.dumpStats(ctx, "typical-contig")
	maxContiguousAllocator.dumpStats(ctx, "contig")
}
