package diff_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/object"
)

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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("dfjlgn"))
	dirObjectID2 := object.DirectObjectID(cid)

	dir1 := createTestDirectory("testDir1", dirModTime, dirOwnerInfo, dirMode, dirObjectID1)
	dir2 := createTestDirectory("testDir2", dirModTime, dirOwnerInfo, dirMode, dirObjectID2)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	snapshotDiffStats := diff.ComparerStats{}
	expectedStats := snapshotDiffOutput(snapshotDiffStats)

	snapshotDiffStats, err = c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("dfjlgn"))
	dirObjectID2 := object.DirectObjectID(cid)

	file1 := &testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"}
	file2 := &testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"}

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID1,
		file1,
		file2,
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID2,
		file1,
		file2,
	)

	diffSnapshotStats := diff.ComparerStats{}
	expectedStats := snapshotDiffOutput(diffSnapshotStats)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("dfjlgn"))
	dirObjectID2 := object.DirectObjectID(cid)

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file3.txt"}, content: "abcdefghij1"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file4.txt"}, content: "klmnopqrstuvwxyz2"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	diffSnapshotStats := diff.ComparerStats{}
	diffSnapshotStats.FileStats.EntriesAdded = 2
	diffSnapshotStats.FileStats.EntriesRemoved = 2
	expectedOutputStats := snapshotDiffOutput(diffSnapshotStats)

	expectedOutput := "added file ./file3.txt (11 bytes)\nadded file ./file4.txt (17 bytes)\n" +
		"removed file ./file1.txt (10 bytes)\n" +
		"removed file ./file2.txt (16 bytes)\n"

	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

	require.NoError(t, err)
	require.Equal(t, expectedOutputStats, actualStats)
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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("dfjlgn"))
	dirObjectID2 := object.DirectObjectID(cid)

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime1,
		dirOwnerInfo,
		dirMode,
		dirObjectID1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime2,
		dirOwnerInfo,
		dirMode,
		dirObjectID2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)

	diffSnapshotStats := diff.ComparerStats{}
	diffSnapshotStats.DirectoryStats.EntriesModified = 1
	expectedOutputStats := snapshotDiffOutput(diffSnapshotStats)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedOutput := ". modification times differ: 2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"
	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
	require.Equal(t, expectedOutputStats, actualStats)
}

func TestCompareDifferentDirectories_FileTimeDiff(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	fileModTime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fileModTime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirModTime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dirOwnerInfo := fs.OwnerInfo{UserID: 1000, GroupID: 1000}
	dirMode := os.FileMode(0o700)

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	OID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("hvhjb"))
	OID2 := object.DirectObjectID(cid)

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		OID1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime1, name: "file1.txt", oid: OID1}, content: "abcdefghij"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		OID2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fileModTime2, name: "file1.txt", oid: OID2}, content: "abcdefghij"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	diffSnapshotStats := diff.ComparerStats{}
	diffSnapshotStats.FileStats.EntriesModified = 1
	expectedOutputStats := snapshotDiffOutput(diffSnapshotStats)

	expectedOutput := "./file1.txt modification times differ: 2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"

	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
	require.Equal(t, expectedOutputStats, actualStats)
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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID1 := object.DirectObjectID(cid)

	cid, _ = index.IDFromHash("i", []byte("dfjlgn"))
	dirObjectID2 := object.DirectObjectID(cid)

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID1,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime1, oid: object.ID{}, owner: fileOwnerinfo1, mode: 0o700}, content: "abcdefghij"},
	)

	dir2 := createTestDirectory(
		"testDir2",
		dirModTime,
		dirOwnerInfo,
		dirMode,
		dirObjectID2,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime2, oid: object.ID{}, owner: fileOwnerinfo2, mode: 0o777}, content: "abcdefghij"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	diffSnapshotStats := diff.ComparerStats{}
	diffSnapshotStats.FileStats.EntriesWithSameOIDButDiffMetadata = 1
	diffSnapshotStats.FileStats.EntriesWithSameOIDButDiffModTime = 1
	diffSnapshotStats.FileStats.EntriesWithSameOIDButDiffMode = 1
	diffSnapshotStats.FileStats.EntriesWithSameOIDButDiffOwnerUser = 1
	diffSnapshotStats.FileStats.EntriesWithSameOIDButDiffOwnerGroup = 1
	expectedOutputStats := snapshotDiffOutput(diffSnapshotStats)

	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedOutputStats, actualStats)
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

	cid, _ := index.IDFromHash("p", []byte("sdkjfn"))
	dirObjectID := object.DirectObjectID(cid)

	dir1 := createTestDirectory(
		"testDir1",
		dirModTime1,
		dirOwnerInfo1,
		dirMode1,
		dirObjectID,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime}, content: "abcdefghij"},
	)

	dir2 := createTestDirectory(
		"testDir2",
		dirModTime2,
		dirOwnerInfo2,
		dirMode2,
		dirObjectID,
		&testFile{testBaseEntry: testBaseEntry{name: "file1.txt", modtime: fileModTime}, content: "abcdefghij"},
	)
	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	diffSnapshotStats := diff.ComparerStats{}
	diffSnapshotStats.DirectoryStats.EntriesWithSameOIDButDiffMetadata = 1
	diffSnapshotStats.DirectoryStats.EntriesWithSameOIDButDiffModTime = 1
	diffSnapshotStats.DirectoryStats.EntriesWithSameOIDButDiffMode = 1
	diffSnapshotStats.DirectoryStats.EntriesWithSameOIDButDiffOwnerUser = 1
	diffSnapshotStats.DirectoryStats.EntriesWithSameOIDButDiffOwnerGroup = 1
	expectedOutputStats := snapshotDiffOutput(diffSnapshotStats)

	snapshotDiffStats, err := c.Compare(ctx, dir1, dir2)
	actualStats := snapshotDiffOutput(snapshotDiffStats)

	require.NoError(t, err)
	require.Empty(t, buf.String())
	require.Equal(t, expectedOutputStats, actualStats)
}

func createTestDirectory(name string, modtime time.Time, owner fs.OwnerInfo, mode os.FileMode, oid object.ID, files ...fs.Entry) *testDirectory {
	return &testDirectory{testBaseEntry: testBaseEntry{modtime: modtime, name: name, owner: owner, mode: mode, oid: oid}, files: files}
}

func snapshotDiffOutput(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
