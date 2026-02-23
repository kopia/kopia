package filesystem

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

type verifySyncBeforeCloseFile struct {
	osWriteFile // +checklocksignore set on instantiation

	notifyClose      func() // +checklocksignore set on instantiation
	notifyDirtyClose func() // +checklocksignore set on instantiation

	mu sync.Mutex
	// +checklocks:mu
	dirty bool
}

func (vf *verifySyncBeforeCloseFile) Write(p []byte) (n int, err error) {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	vf.dirty = true

	return vf.osWriteFile.Write(p)
}

func (vf *verifySyncBeforeCloseFile) Sync() error {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	err := vf.osWriteFile.Sync()
	if err == nil {
		vf.dirty = false
	}

	return err
}

func (vf *verifySyncBeforeCloseFile) Close() error {
	dirty, err := func() (bool, error) {
		vf.mu.Lock()
		defer vf.mu.Unlock()

		return vf.dirty, vf.osWriteFile.Close()
	}()

	if dirty {
		vf.notifyDirtyClose()
	}

	vf.notifyClose()

	return err
}

type mockOSForSyncTest struct {
	mockOS

	fileOpenCount  atomic.Uint32
	fileCloseCount atomic.Uint32
	dirtyClose     atomic.Bool
}

func (osi *mockOSForSyncTest) Open(fname string) (osReadFile, error) {
	f, err := osi.mockOS.Open(fname)
	if err != nil {
		return nil, err
	}

	osi.fileOpenCount.Add(1)

	return f, nil
}

func (osi *mockOSForSyncTest) CreateNewFile(fname string, perm os.FileMode) (osWriteFile, error) {
	wf, err := osi.mockOS.CreateNewFile(fname, perm)
	if err != nil {
		return nil, err
	}

	osi.fileOpenCount.Add(1)

	return &verifySyncBeforeCloseFile{
		osWriteFile:      wf,
		notifyClose:      func() { osi.fileCloseCount.Add(1) },
		notifyDirtyClose: func() { osi.dirtyClose.Store(true) },
	}, nil
}

// These tests reuse the retry/error-count mock to assert sync handling in PutBlob.
func TestPutBlob_SyncBeforeClose(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	osi := &mockOSForSyncTest{
		mockOS: mockOS{
			osInterface: realOS{},
		},
	}

	st, err := New(ctx, &Options{
		Path:    testutil.TempDirectory(t),
		Options: sharded.Options{DirectoryShards: []int{1}},

		osInterfaceOverride: osi,
	}, true)
	require.NoError(t, err)

	t.Cleanup(func() { _ = st.Close(ctx) })

	err = st.PutBlob(ctx, "blob-sync-ok", gather.FromSlice([]byte("hello")), blob.PutOptions{})

	require.False(t, osi.dirtyClose.Load(), "close called without calling sync after a write")
	require.Equal(t, osi.fileOpenCount.Load(), osi.fileCloseCount.Load(), "calls to file.Close() must match number of opened files()")
	require.NoError(t, err)

	var buf gather.WriteBuffer
	t.Cleanup(buf.Close)

	err = st.GetBlob(ctx, "blob-sync-ok", 0, -1, &buf)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), buf.ToByteSlice())
}

func TestPutBlob_FailsOnSyncError(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	st, err := New(ctx, &Options{
		Path:    dataDir,
		Options: sharded.Options{DirectoryShards: []int{1}},

		osInterfaceOverride: osi,
	}, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(ctx) })

	// Test HACK: write a dummy blob to force writing the sharding configuration file, so writing the
	// config file does not interfere with the test. While this is coupled to the specifics of the
	// current implementation, it is required to be able to test the failure case.
	err = st.PutBlob(ctx, "dummy", gather.FromSlice([]byte("hello")), blob.PutOptions{})
	require.NoError(t, err)

	// Inject a failure per create (re-)try, 10 is the default number of retries
	osi.writeFileSyncRemainingErrors.Store(10)

	err = st.PutBlob(ctx, "blob-sync-fail", gather.FromSlice([]byte("hello")), blob.PutOptions{})
	require.Error(t, err)
	require.ErrorContains(t, err, "can't sync temporary file data")

	_, err = st.GetMetadata(ctx, "blob-sync-fail")
	require.ErrorIs(t, err, blob.ErrBlobNotFound)
}
