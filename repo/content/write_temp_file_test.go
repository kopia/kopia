package content

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestWriteTempFileAtomic_HappyPath verifies that writeTempFileAtomic writes
// the expected content and returns a valid file path.
func TestWriteTempFileAtomic_HappyPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	data := []byte("index-blob-content")

	name, err := writeTempFileAtomic(localFS{}, dir, data)
	require.NoError(t, err)
	require.NotEmpty(t, name)

	// File must exist under the given directory.
	require.Equal(t, dir, filepath.Dir(name))

	got, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestWriteTempFileAtomic_EmptyData verifies that an empty payload is written
// without error and produces a valid (zero-byte) file.
func TestWriteTempFileAtomic_EmptyData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	name, err := writeTempFileAtomic(localFS{}, dir, []byte{})
	require.NoError(t, err)

	info, err := os.Stat(name)
	require.NoError(t, err)
	require.EqualValues(t, 0, info.Size())
}

// TestWriteTempFileAtomic_CreatesDirectoryIfMissing verifies that
// writeTempFileAtomic creates the target directory when it does not exist,
// matching the MkdirAll fallback path.
func TestWriteTempFileAtomic_CreatesDirectoryIfMissing(t *testing.T) {
	t.Parallel()

	// Use a path that does not yet exist.
	dir := filepath.Join(t.TempDir(), "new", "nested", "dir")

	data := []byte("hello")

	name, err := writeTempFileAtomic(localFS{}, dir, data)
	require.NoError(t, err)
	require.Equal(t, dir, filepath.Dir(name))

	got, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestWriteTempFileAtomic_NonExistentDirUnwritable verifies that an error is
// returned when the directory cannot be created (e.g. parent is read-only).
// Skipped on platforms where root may bypass permissions.
func TestWriteTempFileAtomic_NonExistentDirUnwritable(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	if runtime.GOOS == "windows" {
		t.Skip("does not work on windows due to chmod")
	}

	t.Parallel()

	// Create a read-only parent so that MkdirAll cannot create the child.
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o555))

	t.Cleanup(func() { os.Chmod(parent, 0o755) }) //nolint:errcheck

	dir := filepath.Join(parent, "child")

	_, err := writeTempFileAtomic(localFS{}, dir, []byte("data"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "can't create tmp file")
}

// TestWriteTempFileAtomic_FileIsSynced verifies that the data written is
// durable: after writeTempFileAtomic returns, re-reading the file on a freshly
// opened handle yields identical bytes. This exercises the Sync() call added
// by the PR — if Sync were absent the content could still be buffered.
//
// On most OSes a successful Sync() is the only way to guarantee this;
// the test is a best-effort correctness check rather than a strict OS
// durability guarantee.
func TestWriteTempFileAtomic_FileIsSynced(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	data := []byte("synced-content")

	name, err := writeTempFileAtomic(localFS{}, dir, data)
	require.NoError(t, err)

	// Open a new handle to avoid OS read-cache of the same descriptor.
	f, err := os.Open(name)
	require.NoError(t, err)

	defer f.Close()

	buf := make([]byte, len(data))
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)
}

// TestWriteTempFileAtomic_NoTempFilesLeft verifies that writeTempFileAtomic
// does not leak the temporary file after a successful call — the caller is
// expected to rename it, but the file descriptor must already be closed.
// We confirm this indirectly: the returned path must be stat-able (file
// exists and is closed) with no other tmp* siblings beyond the returned one.
func TestWriteTempFileAtomic_NoTempFilesLeft(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	name, err := writeTempFileAtomic(localFS{}, dir, []byte("data"))
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	// Only one file should exist: the one returned.
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(name), entries[0].Name())
}

type mockfs struct {
	localFS

	createWrapper func(file) file
}

func (m mockfs) CreateTemp(dir, pattern string) (file, error) {
	f, err := m.localFS.CreateTemp(dir, pattern)

	if m.createWrapper != nil {
		f = m.createWrapper(f)
	}

	return f, err
}

type mockFileWriteError struct {
	file
}

func (mf mockFileWriteError) Write(p []byte) (n int, err error) {
	return 0, errors.New("mock file write error")
}

type mockFileSyncError struct {
	file
}

func (mf mockFileSyncError) Sync() error {
	return errors.New("mock file sync error")
}

type mockFileCloseError struct {
	file
}

func (mf mockFileCloseError) Close() error {
	if err := mf.file.Close(); err != nil {
		return err
	}

	return errors.New("mock file close error")
}

// TestWriteTempFileAtomic_NoTempFilesLeftOnWriteError verifies that writeTempFileAtomic
// does not leak the temporary file after an error writing the file.
func TestWriteTempFileAtomic_NoTempFilesLeftOnError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mockfs
		description string
	}{
		{
			description: "write-error",
			mockfs: mockfs{
				createWrapper: func(f file) file { return mockFileWriteError{file: f} },
			},
		},
		{
			description: "sync-error",
			mockfs: mockfs{
				createWrapper: func(f file) file { return mockFileSyncError{file: f} },
			},
		},
		{
			description: "close-error",
			mockfs: mockfs{
				createWrapper: func(f file) file { return mockFileCloseError{file: f} },
			},
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			dir := t.TempDir()

			name, err := writeTempFileAtomic(c.mockfs, dir, []byte("data"))
			require.Error(t, err)
			require.Empty(t, name)
			t.Log("error:", err)

			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}
