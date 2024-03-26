package bigmap_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestGrowingSet(t *testing.T) {
	ctx := testlogging.Context(t)

	impl, err := bigmap.NewSetWithOptions(ctx, &bigmap.Options{
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
		var keybuf [sha256.Size]byte

		k := sha256Key(h, keybuf[:0], i)

		require.True(t, impl.Put(ctx, k))
		require.False(t, impl.Put(ctx, k))

		// ensure that previously written key is still there.
		pkindex := i / 2
		pk := sha256Key(h, keybuf[:0], pkindex)
		require.True(t, impl.Contains(pk))

		// ensure that key not written yet is not there.
		nk := sha256Key(h, keybuf[:0], i+1)

		require.False(t, impl.Contains(nk))
	}
}

func BenchmarkSet(b *testing.B) {
	ctx := testlogging.Context(b)
	m, err := bigmap.NewSet(ctx)
	require.NoError(b, err)

	defer m.Close(ctx)

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

		m.Put(ctx, key)
	}

	for range 4 {
		for i := range b.N {
			// generate key=sha256(i) without allocations.
			h.Reset()
			binary.LittleEndian.PutUint64(num[:], uint64(i))
			h.Write(num[:])
			key := h.Sum(keyBuf[:0])

			require.True(b, m.Contains(key))
		}
	}
}

func TestSetPanics(t *testing.T) {
	ctx := testlogging.Context(t)

	m, err := bigmap.NewSet(ctx)
	require.NoError(t, err)

	// too short keys
	require.Panics(t, func() { m.Put(ctx, nil) })
	require.Panics(t, func() { m.Put(ctx, []byte{1}) })

	// too long key
	require.Panics(t, func() { m.Put(ctx, bytes.Repeat([]byte{1}, 256)) })
}
