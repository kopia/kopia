package splitter

import (
	"io"
	"sync"

	"github.com/chmduquesne/rollinghash/v4"
	"github.com/chmduquesne/rollinghash/v4/buzhash32"
	"github.com/chmduquesne/rollinghash/v4/rabinkarp64"
)

// ChunkSplitter splits a byte stream into content-defined chunks. Where
// Splitter is handed a buffer and returns the offset at which the caller
// should cut it, ChunkSplitter owns the chunk accumulator: the caller
// feeds the stream through Write, drains completed chunks through Next and
// Bytes, then calls Close to release the final, possibly short, chunk.
//
// This inversion lets the rolling-hash splitters find boundaries with
// rollinghash.ChunkWriter, which scans buffers with BatchBoundaries and
// skips the sub-minimum-size prefix of every chunk instead of hashing one
// byte at a time. Measured at 2-3x the throughput of the equivalent
// Splitter (BenchmarkSplitter, BenchmarkObjectWriterSplitting).
//
// For a given algorithm and average size the boundaries are identical to
// the equivalent Splitter for every chunk at least splitterSlidingWindowSize
// bytes long, which covers every production splitter (minimum chunk size
// 64 KiB and up); see TestChunkSplitterMatchesSplitter.
type ChunkSplitter interface {
	// Write feeds stream bytes. It consumes all of p and returns no error
	// before Close.
	io.Writer

	// Close signals the end of the stream so that Next can yield the
	// final, possibly short, chunk. Write must not be called afterwards.
	io.Closer

	// Next reports whether another completed chunk is available. Bytes
	// returns the current one, valid only until the next call to Write,
	// Next or Close.
	Next() bool
	Bytes() []byte

	// MaxSegmentSize returns the largest chunk the splitter will emit.
	MaxSegmentSize() int

	// Reset returns the splitter to its initial state, ready for a new
	// stream and re-enabling Write after Close. Any chunk not yet drained
	// is discarded.
	Reset()
}

// ChunkFactory creates ChunkSplitter instances.
type ChunkFactory func() ChunkSplitter

// chunkSplitterFactories maps algorithm names to a ChunkFactory, for the
// subset of SupportedAlgorithms backed by a rolling hash. Names absent
// here (the FIXED family) have no push-based implementation and are only
// available through GetFactory.
//
//nolint:gochecknoglobals
var chunkSplitterFactories = map[string]ChunkFactory{
	"DYNAMIC-128K-BUZHASH": newBuzHash32ChunkFactory(splitterSize128KB),
	"DYNAMIC-256K-BUZHASH": newBuzHash32ChunkFactory(splitterSize256KB),
	"DYNAMIC-512K-BUZHASH": newBuzHash32ChunkFactory(splitterSize512KB),
	"DYNAMIC-1M-BUZHASH":   newBuzHash32ChunkFactory(splitterSize1MB),
	"DYNAMIC-2M-BUZHASH":   newBuzHash32ChunkFactory(splitterSize2MB),
	"DYNAMIC-4M-BUZHASH":   newBuzHash32ChunkFactory(splitterSize4MB),
	"DYNAMIC-8M-BUZHASH":   newBuzHash32ChunkFactory(splitterSize8MB),

	"DYNAMIC-128K-RABINKARP": newRabinKarp64ChunkFactory(splitterSize128KB),
	"DYNAMIC-256K-RABINKARP": newRabinKarp64ChunkFactory(splitterSize256KB),
	"DYNAMIC-512K-RABINKARP": newRabinKarp64ChunkFactory(splitterSize512KB),
	"DYNAMIC-1M-RABINKARP":   newRabinKarp64ChunkFactory(splitterSize1MB),
	"DYNAMIC-2M-RABINKARP":   newRabinKarp64ChunkFactory(splitterSize2MB),
	"DYNAMIC-4M-RABINKARP":   newRabinKarp64ChunkFactory(splitterSize4MB),
	"DYNAMIC-8M-RABINKARP":   newRabinKarp64ChunkFactory(splitterSize8MB),

	"DYNAMIC": newBuzHash32ChunkFactory(splitterSize4MB),
}

// GetChunkFactory returns the ChunkFactory for the named algorithm, or nil
// if the algorithm has no push-based implementation (in which case
// GetFactory still returns a byte-at-a-time Splitter).
func GetChunkFactory(name string) ChunkFactory {
	return chunkSplitterFactories[name]
}

// rollingHashChunkSplitter adapts rollinghash.ChunkWriter to ChunkSplitter.
//
// A stream shorter than the rolling window yields no boundary; since
// rollinghash v4.3.3 the ChunkWriter still emits those trailing bytes as a
// single final chunk (a truly empty stream yields nothing), so no
// short-stream fallback is needed here.
type rollingHashChunkSplitter struct {
	cw      rollinghash.ChunkWriter
	maxSize int
}

func (s *rollingHashChunkSplitter) Write(p []byte) (int, error) {
	//nolint:wrapcheck
	return s.cw.Write(p)
}

func (s *rollingHashChunkSplitter) Close() error {
	//nolint:wrapcheck
	return s.cw.Close()
}

func (s *rollingHashChunkSplitter) Next() bool {
	return s.cw.Next()
}

func (s *rollingHashChunkSplitter) Bytes() []byte {
	return s.cw.Bytes()
}

func (s *rollingHashChunkSplitter) MaxSegmentSize() int {
	return s.maxSize
}

func (s *rollingHashChunkSplitter) Reset() {
	s.cw.Reset()
}

// recyclableChunkSplitter returns its wrapped splitter to a pool when
// Reset is called, so that the sizeable rolling-hash chunk buffer is
// reused across object writers.
type recyclableChunkSplitter struct {
	*rollingHashChunkSplitter

	pool *sync.Pool
}

func (s *recyclableChunkSplitter) Reset() {
	s.rollingHashChunkSplitter.Reset()
	s.pool.Put(s.rollingHashChunkSplitter)
}

func pooledChunk(newSplitter func() *rollingHashChunkSplitter) ChunkFactory {
	pool := &sync.Pool{}

	return func() ChunkSplitter {
		s, ok := pool.Get().(*rollingHashChunkSplitter)
		if !ok {
			s = newSplitter()
		}

		return &recyclableChunkSplitter{s, pool}
	}
}

func newBuzHash32ChunkFactory(avgSize int) ChunkFactory {
	mask := uint64(uint32(avgSize - 1))      //nolint:gosec
	minSize, maxSize := avgSize/2, avgSize*2 //nolint:mnd

	return pooledChunk(func() *rollingHashChunkSplitter {
		return &rollingHashChunkSplitter{
			cw: rollinghash.NewChunkWriter(buzhash32.New(), splitterSlidingWindowSize, mask,
				rollinghash.WithBoundaries(minSize, maxSize)),
			maxSize: maxSize,
		}
	})
}

func newRabinKarp64ChunkFactory(avgSize int) ChunkFactory {
	mask := uint64(avgSize - 1)              //nolint:gosec
	minSize, maxSize := avgSize/2, avgSize*2 //nolint:mnd

	return pooledChunk(func() *rollingHashChunkSplitter {
		return &rollingHashChunkSplitter{
			cw: rollinghash.NewChunkWriter(rabinkarp64.New(), splitterSlidingWindowSize, mask,
				rollinghash.WithBoundaries(minSize, maxSize)),
			maxSize: maxSize,
		}
	})
}
