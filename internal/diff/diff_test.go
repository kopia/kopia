package diff_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

const statsOnly = false

var (
	_ fs.Entry     = (*testFile)(nil)
	_ fs.Directory = (*testDirectory)(nil)
)

type testBaseEntry struct {
	modtime time.Time
	mode    os.FileMode
	name    string
	owner   fs.OwnerInfo
	oid     object.ID
}

func (f *testBaseEntry) IsDir() bool                 { return false }
func (f *testBaseEntry) LocalFilesystemPath() string { return f.name }
func (f *testBaseEntry) Close()                      {}
func (f *testBaseEntry) Name() string                { return f.name }
func (f *testBaseEntry) ModTime() time.Time          { return f.modtime }
func (f *testBaseEntry) Sys() interface{}            { return nil }
func (f *testBaseEntry) Owner() fs.OwnerInfo         { return f.owner }
func (f *testBaseEntry) Device() fs.DeviceInfo       { return fs.DeviceInfo{Dev: 1} }
func (f *testBaseEntry) ObjectID() object.ID         { return f.oid }

func (f *testBaseEntry) Mode() os.FileMode {
	if f.mode == 0 {
		return 0o644
	}

	return f.mode & ^os.ModeDir
}

type testFile struct {
	testBaseEntry
	content string
}

func (f *testFile) Open(ctx context.Context) (io.Reader, error) {
	return strings.NewReader(f.content), nil
}

func (f *testFile) Size() int64 { return int64(len(f.content)) }

type testDirectory struct {
	testBaseEntry
	files []fs.Entry
}

func (d *testDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	return fs.StaticIterator(d.files, nil), nil
}

func (d *testDirectory) SupportsMultipleIterations() bool                { return false }
func (d *testDirectory) IsDir() bool                                     { return true }
func (d *testDirectory) LocalFilesystemPath() string                     { return d.name }
func (d *testDirectory) Size() int64                                     { return 0 }
func (d *testDirectory) Readdir(ctx context.Context) ([]fs.Entry, error) { return d.files, nil }

func (d *testDirectory) Mode() os.FileMode {
	if d.mode == 0 {
		return os.ModeDir | 0o755
	}

	return os.ModeDir | d.mode
}

func (d *testDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	for _, f := range d.files {
		if f.Name() == name {
			return f, nil
		}
	}

	return nil, fs.ErrEntryNotFound
}

func TestCompareEmptyDirectories(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o777)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "dfjlgn")
	dir1 := createTestDirectory("testDir1", dirModTime, dirOwnerInfo, dirMode, oid1)
	dir2 := createTestDirectory("testDir2", dirModTime, dirOwnerInfo, dirMode, oid2)

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedStats := diff.Stats{}
	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func TestCompareIdenticalDirectories(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o777)
	fileModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "dfjlgn")

	file1 := &testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"}
	file2 := &testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"}

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid1,
		file1,
		file2,
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid2,
		file1,
		file2,
	)

	expectedStats := diff.Stats{}

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func TestCompareDifferentDirectories(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fileModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o777)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "dfjlgn")

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file3.txt"}, content: "abcdefghij1"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file4.txt"}, content: "klmnopqrstuvwxyz2"},
	)

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedStats := diff.Stats{}
	expectedStats.FileEntries.Added = 2
	expectedStats.FileEntries.Removed = 2

	expectedOutput := "added file ./file3.txt (11 bytes)\nadded file ./file4.txt (17 bytes)\n" +
		"removed file ./file1.txt (10 bytes)\n" +
		"removed file ./file2.txt (16 bytes)\n"

	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Equal(t, expectedStats, actualStats)
	require.Equal(t, expectedOutput, buf.String())
}

func TestCompareDifferentDirectories_DirTimeDiff(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	fileModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirModTime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirModTime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o777)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "dfjlgn")

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime1,
		dirOwnerInfo,
		dirMode,
		oid1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime2,
		dirOwnerInfo,
		dirMode,
		oid2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)

	expectedStats := diff.Stats{}
	expectedStats.DirectoryEntries.Modified = 1

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedOutput := ". modification times differ: 2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"
	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func TestCompareDifferentDirectories_FileTimeDiff(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	fileModTime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fileModTime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o700)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "hvhjb")

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime1, name: "file1.txt", oid: oid1}, content: "abcdefghij"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime2, name: "file1.txt", oid: oid2}, content: "abcdefghij"},
	)

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedStats := diff.Stats{}
	expectedStats.FileEntries.Modified = 1

	expectedOutput := "./file1.txt modification times differ: 2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"

	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func TestCompareFileWithIdenticalContentsButDiffFileMetadata(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	fileModTime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fileModTime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)

	fileOwnerinfo1 := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	fileOwnerinfo2 := fs.OwnerInfo{UserID: 1001, GroupID: 1002}

	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o777)
	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)

	oid1 := oidForString(t, "k", "sdkjfn")
	oid2 := oidForString(t, "k", "dfjlgn")

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid1,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime1, oid: object.ID{}, owner: fileOwnerinfo1, mode: 0o700}, content: "abcdefghij"},
	)

	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		oid2,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime2, oid: object.ID{}, owner: fileOwnerinfo2, mode: 0o777}, content: "abcdefghij"},
	)

	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedStats := diff.Stats{
		FileEntries: diff.EntryTypeStats{
			SameContentButDifferentMetadata:         1,
			SameContentButDifferentModificationTime: 1,
			SameContentButDifferentMode:             1,
			SameContentButDifferentUserOwner:        1,
			SameContentButDifferentGroupOwner:       1,
		},
	}

	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func TestCompareIdenticalDirectoriesWithDiffDirectoryMetadata(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dirModTime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirModTime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)

	dirOwnerInfo1 := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirOwnerInfo2 := fs.OwnerInfo{UserID: 1001, GroupID: 1002}

	dirMode1 := os.FileMode(0o644)
	dirMode2 := os.FileMode(0o777)

	fileModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)

	oid := oidForString(t, "k", "sdkjfn")

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime1,
		dirOwnerInfo1,
		dirMode1,
		oid,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime}, content: "abcdefghij"},
	)

	dir2 := createTestDirectory(
		"testDir2",
		dirModTime2,
		dirOwnerInfo2,
		dirMode2,
		oid,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime}, content: "abcdefghij"},
	)
	c, err := diff.NewComparer(&buf, statsOnly)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedStats := diff.Stats{
		DirectoryEntries: diff.EntryTypeStats{
			SameContentButDifferentMetadata:         1,
			SameContentButDifferentModificationTime: 1,
			SameContentButDifferentMode:             1,
			SameContentButDifferentUserOwner:        1,
			SameContentButDifferentGroupOwner:       1,
		},
	}

	actualStats, err := c.Compare(ctx, dir1, dir2)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedStats, actualStats)
}

func createTestDirectory(name string, modtime time.Time, owner fs.OwnerInfo, mode os.FileMode, oid object.ID, files ...fs.Entry) *testDirectory {
	return &testDirectory{testBaseEntry: testBaseEntry{modtime: modtime, name: name, owner: owner, mode: mode, oid: oid}, files: files}
}

func getManifests(t *testing.T) map[string]*snapshot.Manifest {
	t.Helper()

	// manifests store snapshot manifests based on start-time
	manifests := make(map[string]*snapshot.Manifest, 3)

	src := getSnapshotSource()
	snapshotTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	rootEntry1 := snapshot.DirEntry{
		ObjectID: oidForString(t, "", "indexID1"),
	}

	rootEntry2 := snapshot.DirEntry{
		ObjectID: oidForString(t, "", "indexID2"),
	}

	manifests["initial_snapshot"] = &snapshot.Manifest{
		ID:          "manifest_1_id",
		Source:      src,
		StartTime:   fs.UTCTimestamp(snapshotTime.Add((-24) * time.Hour).UnixNano()),
		Description: "snapshot captured a day ago",
		RootEntry:   &rootEntry2,
	}

	manifests["intermediate_snapshot"] = &snapshot.Manifest{
		ID:          "manifest_2_id",
		Source:      src,
		StartTime:   fs.UTCTimestamp(snapshotTime.Add(-time.Hour).UnixNano()),
		Description: "snapshot taken an hour ago",
		RootEntry:   &rootEntry2,
	}

	manifests["latest_snapshot"] = &snapshot.Manifest{
		ID:          "manifest_3_id",
		Source:      src,
		StartTime:   fs.UTCTimestamp(snapshotTime.UnixNano()),
		Description: "latest snapshot",
		RootEntry:   &rootEntry1,
	}

	return manifests
}

// Tests GetPrecedingSnapshot function
//   - GetPrecedingSnapshot with an invalid snapshot id and expect an error;
//   - Add a snapshot, expect an error from GetPreceedingSnapshot since there is
//     only a single snapshot in the repo;
//   - Subsequently add more snapshots and GetPreceedingSnapshot theimmediately
//     preceding with no error.
func TestGetPrecedingSnapshot(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	manifests := getManifests(t)

	_, err := diff.GetPrecedingSnapshot(ctx, env.RepositoryWriter, "non_existent_snapshot_ID")
	require.Error(t, err, "expect error when calling GetPrecedingSnapshot with a wrong snapshotID")

	initialSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["initial_snapshot"])
	_, err = diff.GetPrecedingSnapshot(ctx, env.RepositoryWriter, string(initialSnapshotManifestID))
	require.Error(t, err, "expect error when there is a single snapshot in the repo")

	intermediateSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["intermediate_snapshot"])
	gotManID, err := diff.GetPrecedingSnapshot(ctx, env.RepositoryWriter, string(intermediateSnapshotManifestID))
	require.NoError(t, err)
	require.Equal(t, initialSnapshotManifestID, gotManID.ID)

	latestSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["latest_snapshot"])
	gotManID2, err := diff.GetPrecedingSnapshot(ctx, env.RepositoryWriter, string(latestSnapshotManifestID))
	require.NoError(t, err)
	require.Equal(t, intermediateSnapshotManifestID, gotManID2.ID)
}

// First call GetTwoLatestSnapshots with insufficient snapshots in the repo and
// expect an error;
// As snapshots are added, GetTwoLatestSnapshots is expected to return the
// manifests for the two most recent snapshots for a the given source.
func TestGetTwoLatestSnapshots(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	snapshotSrc := getSnapshotSource()
	manifests := getManifests(t)

	_, _, err := diff.GetTwoLatestSnapshotsForASource(ctx, env.RepositoryWriter, snapshotSrc)
	require.Error(t, err, "expected error as there aren't enough snapshots to get the two most recent snapshots")

	initialSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["initial_snapshot"])
	_, _, err = diff.GetTwoLatestSnapshotsForASource(ctx, env.RepositoryWriter, snapshotSrc)
	require.Error(t, err, "expected error as there aren't enough snapshots to get the two most recent snapshots")

	intermediateSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["intermediate_snapshot"])

	var expectedManifestIDs []manifest.ID
	expectedManifestIDs = append(expectedManifestIDs, initialSnapshotManifestID, intermediateSnapshotManifestID)

	secondLastSnapshot, lastSnapshot, err := diff.GetTwoLatestSnapshotsForASource(ctx, env.RepositoryWriter, snapshotSrc)

	var gotManifestIDs []manifest.ID
	gotManifestIDs = append(gotManifestIDs, secondLastSnapshot.ID, lastSnapshot.ID)

	require.NoError(t, err)
	require.Equal(t, expectedManifestIDs, gotManifestIDs)

	latestSnapshotManifestID := mustSaveSnapshot(t, env.RepositoryWriter, manifests["latest_snapshot"])

	expectedManifestIDs = nil
	expectedManifestIDs = append(expectedManifestIDs, intermediateSnapshotManifestID, latestSnapshotManifestID)

	gotManifestIDs = nil
	secondLastSnapshot, lastSnapshot, err = diff.GetTwoLatestSnapshotsForASource(ctx, env.RepositoryWriter, snapshotSrc)
	gotManifestIDs = append(gotManifestIDs, secondLastSnapshot.ID, lastSnapshot.ID)

	require.NoError(t, err)
	require.Equal(t, expectedManifestIDs, gotManifestIDs)
}

func mustSaveSnapshot(t *testing.T, rep repo.RepositoryWriter, man *snapshot.Manifest) manifest.ID {
	t.Helper()

	id, err := snapshot.SaveSnapshot(testlogging.Context(t), rep, man)
	require.NoError(t, err, "saving snapshot")

	return id
}

func getSnapshotSource() snapshot.SourceInfo {
	src := snapshot.SourceInfo{
		Host:     "host-1",
		UserName: "user-1",
		Path:     "/some/path",
	}

	return src
}

func oidForString(t *testing.T, prefix content.IDPrefix, s string) object.ID {
	t.Helper()

	return oidForContent(t, prefix, []byte(s))
}

func oidForContent(t *testing.T, prefix content.IDPrefix, c []byte) object.ID {
	t.Helper()

	h := blake3.New()
	_, err := h.Write(c)

	require.NoError(t, err)

	cid, err := content.IDFromHash(prefix, h.Sum(nil))
	require.NoError(t, err)

	return object.DirectObjectID(cid)
}
