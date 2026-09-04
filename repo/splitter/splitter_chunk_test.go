package splitter

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// splitOffsets returns the absolute cut offsets produced by feeding all of
// data to a byte-at-a-time Splitter.
func splitOffsets(data []byte, s Splitter) []int {
	var offsets []int

	total := len(data)
	pos := 0

	for len(data) > 0 {
		n := s.NextSplitPoint(data)
		if n < 0 {
			break
		}

		pos += n
		offsets = append(offsets, pos)
		data = data[n:]
	}

	// the trailing bytes after the last boundary are a final chunk that
	// NextSplitPoint never reports (its caller flushes them itself).
	if pos < total {
		offsets = append(offsets, total)
	}

	return offsets
}

// chunkOffsets returns the absolute cut offsets produced by feeding data to
// a ChunkSplitter in pieces of the given size (0 means one Write), plus the
// concatenation of every emitted chunk.
func chunkOffsets(tb testing.TB, data []byte, s ChunkSplitter, pieceSize int) (offsets []int, assembled []byte) {
	tb.Helper()

	if pieceSize <= 0 {
		pieceSize = len(data)
	}

	pos := 0

	drain := func() {
		for s.Next() {
			b := s.Bytes()
			pos += len(b)
			offsets = append(offsets, pos)
			assembled = append(assembled, b...)
		}
	}

	for off := 0; off < len(data); off += pieceSize {
		end := min(off+pieceSize, len(data))

		if _, err := s.Write(data[off:end]); err != nil {
			tb.Fatalf("write: %v", err)
		}

		drain()
	}

	if err := s.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}

	drain()

	return offsets, assembled
}

// TestChunkSplitterMatchesSplitter verifies that the push-based rolling-hash
// splitters cut a stream at exactly the same offsets as the equivalent
// byte-at-a-time Splitter, for every average size whose minimum chunk
// length is at least the rolling window. All production splitters
// (minimum 64 KiB) are in this range; the byte-at-a-time splitters prime
// the window with zeros, which only changes boundaries in the first
// splitterSlidingWindowSize bytes, below any real minimum chunk size.
func TestChunkSplitterMatchesSplitter(t *testing.T) {
	r := rand.New(rand.NewSource(5))

	data := make([]byte, 5000000)
	if _, err := r.Read(data); err != nil {
		t.Fatalf("cannot initialize random data: %v", err)
	}

	avgSizes := []int{
		1024, 2048, 32768, 65536,
		splitterSize128KB, splitterSize256KB, splitterSize512KB,
		splitterSize1MB, splitterSize2MB,
	}

	pieceSizes := []int{0, 1, 100, 4096, 65537}

	algos := []struct {
		name string
		pull func(int) Factory
		push func(int) ChunkFactory
	}{
		{"buzhash32", newBuzHash32SplitterFactory, newBuzHash32ChunkFactory},
		{"rabinkarp64", newRabinKarp64SplitterFactory, newRabinKarp64ChunkFactory},
	}

	for _, algo := range algos {
		for _, avg := range avgSizes {
			want := splitOffsets(data, algo.pull(avg)())

			for _, piece := range pieceSizes {
				t.Run(fmt.Sprintf("%s/%d/piece-%d", algo.name, avg, piece), func(t *testing.T) {
					t.Parallel()

					got, assembled := chunkOffsets(t, data, algo.push(avg)(), piece)

					require.Equal(t, want, got, "cut offsets differ")
					require.True(t, bytes.Equal(data, assembled), "assembled stream differs from input")
				})
			}
		}
	}
}

func TestChunkSplitterShortStreams(t *testing.T) {
	for _, size := range []int{0, 1, 63, 64, 65, 1000} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i)
			}

			_, assembled := chunkOffsets(t, data, newBuzHash32ChunkFactory(splitterSize128KB)(), 0)

			require.True(t, bytes.Equal(data, assembled), "assembled stream differs from input")
		})
	}
}

func TestChunkSplitterMaxSegmentSize(t *testing.T) {
	require.Equal(t, splitterSize1MB*2, newBuzHash32ChunkFactory(splitterSize1MB)().MaxSegmentSize())
	require.Equal(t, splitterSize1MB*2, newRabinKarp64ChunkFactory(splitterSize1MB)().MaxSegmentSize())
}

func BenchmarkSplitter(b *testing.B) {
	r := rand.New(rand.NewSource(5))

	// feed the splitters the same way the object writer does: 64 KiB
	// pieces, draining completed chunks between them.
	const pieceSize = 64 << 10

	data := make([]byte, 64<<20)
	if _, err := r.Read(data); err != nil {
		b.Fatalf("cannot initialize random data: %v", err)
	}

	for _, algo := range []string{"DYNAMIC-4M-BUZHASH", "DYNAMIC-2M-RABINKARP"} {
		b.Run(algo+"/splitter", func(b *testing.B) {
			b.SetBytes(int64(len(data)))

			for b.Loop() {
				s := GetFactory(algo)()

				for off := 0; off < len(data); off += pieceSize {
					buf := data[off:min(off+pieceSize, len(data))]
					for len(buf) > 0 {
						n := s.NextSplitPoint(buf)
						if n < 0 {
							break
						}

						buf = buf[n:]
					}
				}

				s.Close()
			}
		})

		b.Run(algo+"/chunksplitter", func(b *testing.B) {
			b.SetBytes(int64(len(data)))

			for b.Loop() {
				s := GetChunkFactory(algo)()

				for off := 0; off < len(data); off += pieceSize {
					if _, err := s.Write(data[off:min(off+pieceSize, len(data))]); err != nil {
						b.Fatalf("write: %v", err)
					}

					for s.Next() { //nolint:revive
					}
				}

				if err := s.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}

				for s.Next() { //nolint:revive
				}

				s.Reset()
			}
		})
	}
}
