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

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/diff"
)

var (
	_ fs.Entry     = (*testFile)(nil)
	_ fs.Directory = (*testDirectory)(nil)
)

type testBaseEntry struct {
	modtime time.Time
	name    string
}

func (f *testBaseEntry) IsDir() bool                 { return false }
func (f *testBaseEntry) LocalFilesystemPath() string { return f.name }
func (f *testBaseEntry) Close()                      {}
func (f *testBaseEntry) Name() string                { return f.name }
func (f *testBaseEntry) Mode() os.FileMode           { return 0o644 }
func (f *testBaseEntry) ModTime() time.Time          { return f.modtime }
func (f *testBaseEntry) Sys() interface{}            { return nil }
func (f *testBaseEntry) Owner() fs.OwnerInfo         { return fs.OwnerInfo{UserID: 1000, GroupID: 1000} }
func (f *testBaseEntry) Device() fs.DeviceInfo       { return fs.DeviceInfo{Dev: 1} }

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
func (d *testDirectory) Mode() os.FileMode                               { return 0o755 }
func (d *testDirectory) Readdir(ctx context.Context) ([]fs.Entry, error) { return d.files, nil }

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

	dmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dir1 := createTestDirectory("testDir1", dmodtime)
	dir2 := createTestDirectory("testDir2", dmodtime)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	err = c.Compare(ctx, dir1, dir2)
	require.NoError(t, err)
	require.Empty(t, buf.String())
}

func TestCompareIdenticalDirectories(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dir1 := createTestDirectory(
		"testDir1",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	err = c.Compare(ctx, dir1, dir2)
	require.NoError(t, err)
	require.Empty(t, buf.String())
}

func TestCompareDifferentDirectories(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dir1 := createTestDirectory(
		"testDir1",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file3.txt"}, content: "abcdefghij1"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file4.txt"}, content: "klmnopqrstuvwxyz2"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedOutput := "added file ./file3.txt (11 bytes)\nadded file ./file4.txt (17 bytes)\n" +
		"removed file ./file1.txt (10 bytes)\n" +
		"removed file ./file2.txt (16 bytes)\n"

	err = c.Compare(ctx, dir1, dir2)
	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
}

func TestCompareDifferentDirectories_DirTimeDiff(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	dmodtime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dmodtime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)
	fmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dir1 := createTestDirectory(
		"testDir1",
		dmodtime1,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime2,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file1.txt"}, content: "abcdefghij"},
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime, name: "file2.txt"}, content: "klmnopqrstuvwxyz"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedOutput := ". modification times differ:  2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"
	err = c.Compare(ctx, dir1, dir2)
	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
}

func TestCompareDifferentDirectories_FileTimeDiff(t *testing.T) {
	var buf bytes.Buffer

	ctx := context.Background()

	fmodtime1 := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	fmodtime2 := time.Date(2022, time.April, 12, 10, 30, 0, 0, time.UTC)
	dmodtime := time.Date(2023, time.April, 12, 10, 30, 0, 0, time.UTC)
	dir1 := createTestDirectory(
		"testDir1",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime1, name: "file1.txt"}, content: "abcdefghij"},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{testBaseEntry: testBaseEntry{modtime: fmodtime2, name: "file1.txt"}, content: "abcdefghij"},
	)

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	expectedOutput := "./file1.txt modification times differ:  2023-04-12 10:30:00 +0000 UTC 2022-04-12 10:30:00 +0000 UTC\n"

	err = c.Compare(ctx, dir1, dir2)
	require.NoError(t, err)
	require.Equal(t, expectedOutput, buf.String())
}

func createTestDirectory(name string, modtime time.Time, files ...fs.Entry) *testDirectory {
	return &testDirectory{testBaseEntry: testBaseEntry{modtime: modtime, name: name}, files: files}
}
