package bigmap_test

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestMap(t *testing.T) {
	ctx := testlogging.Context(t)

	impl, err := bigmap.NewMap(ctx)
	require.NoError(t, err)

	defer impl.Close(ctx)

	key1 := []byte("key1")
	key2 := []byte("longerkey2")
	val1 := []byte("val1")
	val2 := []byte("val2")

	v, ok := impl.Get(nil, key1)
	require.Nil(t, v)
	require.False(t, ok)

	impl.PutIfAbsent(ctx, key1, val1)

	v, ok = impl.Get(nil, key1)
	require.True(t, ok)
	require.Equal(t, val1, v)

	v, ok = impl.Get(nil, key2)
	require.Nil(t, v)
	require.False(t, ok)

	impl.PutIfAbsent(ctx, key2, val2)

	v, ok = impl.Get(nil, key2)
	require.True(t, ok)
	require.Equal(t, val2, v)

	v, ok = impl.Get(nil, key1)
	require.True(t, ok)
	require.Equal(t, val1, v)
}

func TestGrowingMap(t *testing.T) {
	ctx := testlogging.Context(t)

	impl, err := bigmap.NewMapWithOptions(ctx, true, &bigmap.Options{
		InitialSizeLogarithm: 9,
		NumMemorySegments:    3,
		MemorySegmentSize:    1000,
		FileSegmentSize:      4 << 20,
	})
	require.NoError(t, err)

	defer impl.Close(ctx)

	h := sha256.New()

	// insert 20K hashes
	for i := 0; i < 20000; i++ {
		var keybuf, valbuf, valbuf2 [sha256.Size]byte

		k := sha256Key(h, keybuf[:0], i)
		v := sha256Key(h, valbuf[:0], i+3)

		require.True(t, impl.PutIfAbsent(ctx, k, v))

		// ensure that previously written key is still there.
		pkindex := i / 2
		pk := sha256Key(h, keybuf[:0], pkindex)
		require.True(t, impl.Contains(pk))

		pv, ok := impl.Get(valbuf2[:0], pk)
		require.True(t, ok)
		require.Equal(t, pv, sha256Key(h, valbuf[:0], pkindex+3))

		// ensure that key not written yet is not there.
		nk := sha256Key(h, keybuf[:0], i+1)

		require.False(t, impl.Contains(nk))

		_, ok2 := impl.Get(valbuf2[:0], nk)
		require.False(t, ok2)
	}
}

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
	for i := 0; i < 20000; i++ {
		var keybuf [sha256.Size]byte

		k := sha256Key(h, keybuf[:0], i)

		require.True(t, impl.Put(ctx, k))

		// ensure that previously written key is still there.
		pkindex := i / 2
		pk := sha256Key(h, keybuf[:0], pkindex)
		require.True(t, impl.Contains(pk))

		// ensure that key not written yet is not there.
		nk := sha256Key(h, keybuf[:0], i+1)

		require.False(t, impl.Contains(nk))
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
	m, err := bigmap.NewMapWithOptions(ctx, false, nil)
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

	for i := 0; i < b.N; i++ {
		// generate key=sha256(i) without allocations.
		h.Reset()
		binary.LittleEndian.PutUint64(num[:], uint64(i))
		h.Write(num[:])
		key := h.Sum(keyBuf[:0])

		m.PutIfAbsent(ctx, key, someVal)
	}

	valBuf := make([]byte, 10)

	for j := 0; j < 4; j++ {
		for i := 0; i < b.N; i++ {
			// generate key=sha256(i) without allocations.
			h.Reset()
			binary.LittleEndian.PutUint64(num[:], uint64(i))
			h.Write(num[:])
			key := h.Sum(keyBuf[:0])

			_, ok := m.Get(valBuf[:0], key)
			require.True(b, ok)
		}
	}
}

func BenchmarkSyncMap_NoValue(b *testing.B) {
	benchmarkSyncMap(b, []byte{})
}

func BenchmarkSyncMap_WithValue(b *testing.B) {
	someVal := []byte{1, 2, 3}

	benchmarkSyncMap(b, someVal)
}

//nolint:thelper
func benchmarkSyncMap(b *testing.B, someVal []byte) {
	var m sync.Map

	var (
		h      = sha256.New()
		num    [8]byte
		keyBuf [sha256.Size]byte
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// generate key=sha256(i) without allocations.
		h.Reset()
		binary.LittleEndian.PutUint64(num[:], uint64(i))
		h.Write(num[:])
		key := h.Sum(keyBuf[:0])

		m.Store(string(key), append([]byte{}, someVal...))
	}

	for j := 0; j < 4; j++ {
		for i := 0; i < b.N; i++ {
			// generate key=sha256(i) without allocations.
			h.Reset()
			binary.LittleEndian.PutUint64(num[:], uint64(i))
			h.Write(num[:])
			key := h.Sum(keyBuf[:0])

			val, ok := m.Load(string(key))
			require.True(b, ok)
			require.Equal(b, someVal, val)
		}
	}
}

func TestErrors(t *testing.T) {
	ctx := testlogging.Context(t)

	_, err := bigmap.NewMapWithOptions(ctx, true, &bigmap.Options{
		InitialSizeLogarithm: 8,
	})

	require.ErrorContains(t, err, "invalid initial size")
}
