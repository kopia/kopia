package upload

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
)

// mockFileBase provides common fs.Entry fields for test mocks.
type mockFileBase struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
}

func (e *mockFileBase) Name() string                { return e.name }
func (e *mockFileBase) IsDir() bool                 { return false }
func (e *mockFileBase) Mode() os.FileMode           { return e.mode }
func (e *mockFileBase) ModTime() time.Time          { return e.modTime }
func (e *mockFileBase) Size() int64                 { return e.size }
func (e *mockFileBase) Sys() any                    { return nil }
func (e *mockFileBase) Owner() fs.OwnerInfo         { return fs.OwnerInfo{} }
func (e *mockFileBase) Device() fs.DeviceInfo       { return fs.DeviceInfo{} }
func (e *mockFileBase) LocalFilesystemPath() string { return "" }
func (e *mockFileBase) Close()                      {}

func (e *mockFileBase) Open(_ context.Context) (fs.Reader, error) {
	return nil, nil
}

// mockFileNoBtime implements fs.File but NOT fs.EntryWithBirthTime,
// simulating a filesystem without btime support (e.g., older Linux kernels < 4.11,
// or filesystems like tmpfs/NFS that don't report STATX_BTIME).
type mockFileNoBtime struct {
	mockFileBase
}

// mockFileWithBtime implements fs.File AND fs.EntryWithBirthTime,
// simulating a filesystem with btime support (e.g., Windows, macOS).
type mockFileWithBtime struct {
	mockFileBase
	birthTime time.Time
}

func (e *mockFileWithBtime) BirthTime() time.Time { return e.birthTime }

// mockCachedFileWithBtime adds HasObjectID to mockFileWithBtime,
// simulating a cached snapshot entry with btime.
type mockCachedFileWithBtime struct {
	mockFileWithBtime
	objectID object.ID
}

func (e *mockCachedFileWithBtime) ObjectID() object.ID { return e.objectID }

// mockCachedFileNoBtime adds HasObjectID to mockFileNoBtime,
// simulating a cached snapshot entry without btime (old Kopia version).
type mockCachedFileNoBtime struct {
	mockFileNoBtime
	objectID object.ID
}

func (e *mockCachedFileNoBtime) ObjectID() object.ID { return e.objectID }

// Verify interface compliance.
var (
	_ fs.File               = &mockFileNoBtime{}
	_ fs.File               = &mockFileWithBtime{}
	_ fs.EntryWithBirthTime = &mockFileWithBtime{}
	_ object.HasObjectID    = &mockCachedFileWithBtime{}
	_ object.HasObjectID    = &mockCachedFileNoBtime{}
)

// TestNewCachedDirEntry_PreservesCachedBtime_WhenCurrentLacksBtime simulates a scenario where
// a previous snapshot captured btime (e.g., on macOS/Windows or Linux ext4/btrfs), but the
// current filesystem doesn't report btime (e.g., tmpfs, NFS, or older kernel). The cached
// btime should be preserved in the new snapshot rather than silently dropped.
func TestNewCachedDirEntry_PreservesCachedBtime_WhenCurrentLacksBtime(t *testing.T) {
	t.Parallel()

	cachedBtime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	mtime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	cached := &mockCachedFileWithBtime{
		mockFileWithBtime: mockFileWithBtime{
			mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
			birthTime:    cachedBtime,
		},
	}

	// Current entry has NO btime (e.g., tmpfs, NFS, or older kernel without STATX_BTIME)
	current := &mockFileNoBtime{
		mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
	}

	de, err := newCachedDirEntry(current, cached, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, de.BirthTime, "cached btime should be preserved when current FS lacks btime")
	require.Equal(t, cachedBtime, de.BirthTime.ToTime().UTC(),
		"preserved btime should match the cached value")
}

// TestNewCachedDirEntry_UsesCurrentBtime_WhenAvailable verifies that when the current
// filesystem provides btime, it takes precedence over cached btime (e.g., user corrected
// a file's creation date).
func TestNewCachedDirEntry_UsesCurrentBtime_WhenAvailable(t *testing.T) {
	t.Parallel()

	cachedBtime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	currentBtime := time.Date(2024, 3, 20, 8, 0, 0, 0, time.UTC)
	mtime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	cached := &mockCachedFileWithBtime{
		mockFileWithBtime: mockFileWithBtime{
			mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
			birthTime:    cachedBtime,
		},
	}

	// Current entry has different btime (e.g., user corrected creation date)
	current := &mockFileWithBtime{
		mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
		birthTime:    currentBtime,
	}

	de, err := newCachedDirEntry(current, cached, "file.txt")
	require.NoError(t, err)
	require.NotNil(t, de.BirthTime, "btime should be set")
	require.Equal(t, currentBtime, de.BirthTime.ToTime().UTC(),
		"current btime should take precedence when available")
}

// TestNewCachedDirEntry_NoBtime_WhenNeitherHasBtime verifies that when neither the current
// nor cached entry has btime (e.g., old Kopia version on a filesystem without btime), btime remains nil.
func TestNewCachedDirEntry_NoBtime_WhenNeitherHasBtime(t *testing.T) {
	t.Parallel()

	mtime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	cached := &mockCachedFileNoBtime{
		mockFileNoBtime: mockFileNoBtime{
			mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
		},
	}

	current := &mockFileNoBtime{
		mockFileBase: mockFileBase{name: "file.txt", mode: 0o644, size: 100, modTime: mtime},
	}

	de, err := newCachedDirEntry(current, cached, "file.txt")
	require.NoError(t, err)
	require.Nil(t, de.BirthTime, "btime should remain nil when neither entry has it")
}
