package object

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
)

// parallelOpener wraps a Manager's contentMgr to satisfy objectOpener.
type parallelOpener struct {
	om *Manager
}

func (p *parallelOpener) OpenObject(ctx context.Context, id ID) (Reader, error) {
	return Open(ctx, p.om.contentMgr, id)
}

func TestReadIndirectObjectParallel_NotIndirect(t *testing.T) {
	ctx := testlogging.Context(t)

	_, _, mgr := setupTest(t, nil)

	w := mgr.NewWriter(ctx, WriterOptions{})
	_, err := w.Write([]byte("hello world"))
	require.NoError(t, err)

	oid, err := w.Result()
	require.NoError(t, err)

	// A small object will be stored as a single non-indirect object.
	if _, isIndirect := oid.IndexObjectID(); isIndirect {
		t.Skip("unexpectedly got an indirect object for tiny data")
	}

	opener := &parallelOpener{om: mgr}
	err = ReadIndirectObjectParallel(ctx, opener, oid, 4, func(int64, []byte) error { return nil })
	require.ErrorIs(t, err, ErrNotParallelizable)
}

func TestReadIndirectObjectParallel_MultiChunk(t *testing.T) {
	ctx := testlogging.Context(t)

	// setupTest uses FIXED-1M splitter; write >2MB to guarantee at least 2 chunks.
	_, _, mgr := setupTest(t, nil)

	fileData := make([]byte, 3_000_000)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}

	w := mgr.NewWriter(ctx, WriterOptions{})
	_, err := w.Write(fileData)
	require.NoError(t, err)

	oid, err := w.Result()
	require.NoError(t, err)

	if _, isIndirect := oid.IndexObjectID(); !isIndirect {
		t.Fatal("expected an indirect object for large data")
	}

	opener := &parallelOpener{om: mgr}

	var mu sync.Mutex
	collected := map[int64][]byte{}

	var callCount atomic.Int32

	err = ReadIndirectObjectParallel(ctx, opener, oid, 4, func(offset int64, chunk []byte) error {
		callCount.Add(1)

		mu.Lock()
		defer mu.Unlock()

		collected[offset] = append([]byte(nil), chunk...)

		return nil
	})
	require.NoError(t, err)
	require.Greater(t, int(callCount.Load()), 1, "expected multiple chunks")

	// Reassemble and compare with original.
	var total int64
	for _, b := range collected {
		total += int64(len(b))
	}

	require.Equal(t, int64(len(fileData)), total)

	assembled := make([]byte, len(fileData))
	for off, b := range collected {
		copy(assembled[off:], b)
	}

	require.Equal(t, fileData, assembled)
}

func TestReadIndirectObjectParallel_CallbackError(t *testing.T) {
	ctx := testlogging.Context(t)

	_, _, mgr := setupTest(t, nil)

	fileData := make([]byte, 3_000_000)

	w := mgr.NewWriter(ctx, WriterOptions{})
	_, err := w.Write(fileData)
	require.NoError(t, err)

	oid, err := w.Result()
	require.NoError(t, err)

	if _, isIndirect := oid.IndexObjectID(); !isIndirect {
		t.Fatal("expected an indirect object for large data")
	}

	opener := &parallelOpener{om: mgr}
	errBoom := fmt.Errorf("boom")

	err = ReadIndirectObjectParallel(ctx, opener, oid, 4, func(int64, []byte) error {
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)
}
