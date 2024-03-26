package bigmap_test

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestGrowingMap(t *testing.T) {
	ctx := testlogging.Context(t)

	impl, err := bigmap.NewMapWithOptions(ctx, &bigmap.Options{
		InitialSizeLogarithm: 9,
		NumMemorySegments:    3,
		MemorySegmentSize:    1000,
		FileSegmentSize:      4 << 20,
	})
	require.NoError(t, err)

	defer impl.Close(ctx)

	h := sha256.New()

	// insert 20K hashes
	for i := range 20000 {
		var keybuf, valbuf, valbuf2 [sha256.Size]byte

		k := sha256Key(h, keybuf[:0], i)
		v := sha256Key(h, valbuf[:0], i+3)

		require.True(t, impl.PutIfAbsent(ctx, k, v))

		// ensure that previously written key is still there.
		pkindex := i / 2
		pk := sha256Key(h, keybuf[:0], pkindex)
		require.True(t, impl.Contains(pk))

		pv, ok, err := impl.Get(ctx, valbuf2[:0], pk)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, pv, sha256Key(h, valbuf[:0], pkindex+3))

		// ensure that key not written yet is not there.
		nk := sha256Key(h, keybuf[:0], i+1)

		require.False(t, impl.Contains(nk))

		_, ok2, err := impl.Get(ctx, valbuf2[:0], nk)
		require.NoError(t, err)
		require.False(t, ok2)
	}
}

func sha256Key(h hash.Hash, out []byte, i int) []byte {
	var num [8]byte

	// generate key=sha256(i) without allocations.
	h.Reset()
	binary.LittleEndian.PutUint64(num[:], uint64(i))
	h.Write(num[:])

	s := h.Sum(out)

	return s
}

func BenchmarkMap_NoValue(b *testing.B) {
	ctx := testlogging.Context(b)
	m, err := bigmap.NewMapWithOptions(ctx, nil)
	require.NoError(b, err)

	defer m.Close(ctx)

	benchmarkMap(b, m, []byte{})
}

func BenchmarkMap_WithValue(b *testing.B) {
	ctx := testlogging.Context(b)
	m, err := bigmap.NewMap(ctx)
	require.NoError(b, err)

	defer m.Close(ctx)

	benchmarkMap(b, m, []byte{1, 2, 3})
}

//nolint:thelper
func benchmarkMap(b *testing.B, m *bigmap.Map, someVal []byte) {
	ctx := testlogging.Context(b)

	b.ResetTimer()

	var (
		h      = sha256.New()
		num    [8]byte
		keyBuf [sha256.Size]byte
	)

	for i := range b.N {
		// generate key=sha256(i) without allocations.
		h.Reset()
		binary.LittleEndian.PutUint64(num[:], uint64(i))
		h.Write(num[:])
		key := h.Sum(keyBuf[:0])

		m.PutIfAbsent(ctx, key, someVal)
	}

	valBuf := make([]byte, 10)

	for range 4 {
		for i := range b.N {
			// generate key=sha256(i) without allocations.
			h.Reset()
			binary.LittleEndian.PutUint64(num[:], uint64(i))
			h.Write(num[:])
			key := h.Sum(keyBuf[:0])

			_, ok, err := m.Get(ctx, valBuf[:0], key)
			require.NoError(b, err)
			require.True(b, ok)
		}
	}
}
