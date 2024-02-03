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

var _ fs.Entry = (*testFile)(nil)

type testFile struct {
	modtime time.Time
	name    string
	content string
}

func (f *testFile) IsDir() bool                 { return false }
func (f *testFile) LocalFilesystemPath() string { return f.name }
func (f *testFile) Close()                      {}
func (f *testFile) Name() string                { return f.name }
func (f *testFile) Size() int64                 { return int64(len(f.content)) }
func (f *testFile) Mode() os.FileMode           { return 0o644 }
func (f *testFile) ModTime() time.Time          { return f.modtime }
func (f *testFile) Sys() interface{}            { return nil }
func (f *testFile) Owner() fs.OwnerInfo         { return fs.OwnerInfo{UserID: 1000, GroupID: 1000} }
func (f *testFile) Device() fs.DeviceInfo       { return fs.DeviceInfo{Dev: 1} }
func (f *testFile) Open(ctx context.Context) (io.Reader, error) {
	return strings.NewReader(f.content), nil
}

var _ fs.Directory = (*testDirectory)(nil)

type testDirectory struct {
	name    string
	files   []fs.Entry
	modtime time.Time
}

func (d *testDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	return fs.StaticIterator(d.files, nil), nil
}

func (d *testDirectory) SupportsMultipleIterations() bool { return false }
func (d *testDirectory) IsDir() bool                      { return true }
func (d *testDirectory) LocalFilesystemPath() string      { return d.name }
func (d *testDirectory) Close()                           {}
func (d *testDirectory) Name() string                     { return d.name }
func (d *testDirectory) Size() int64                      { return 0 }
func (d *testDirectory) Mode() os.FileMode                { return 0o755 }
func (d *testDirectory) ModTime() time.Time               { return d.modtime }
func (d *testDirectory) Sys() interface{}                 { return nil }
func (d *testDirectory) Owner() fs.OwnerInfo              { return fs.OwnerInfo{UserID: 1000, GroupID: 1000} }
func (d *testDirectory) Device() fs.DeviceInfo            { return fs.DeviceInfo{Dev: 1} }
func (d *testDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	for _, f := range d.files {
		if f.Name() == name {
			return f, nil
		}
	}

	return nil, fs.ErrEntryNotFound
}
func (d *testDirectory) Readdir(ctx context.Context) ([]fs.Entry, error) { return d.files, nil }

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
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime},
		&testFile{name: "file2.txt", content: "klmnopqrstuvwxyz", modtime: fmodtime},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime},
		&testFile{name: "file2.txt", content: "klmnopqrstuvwxyz", modtime: fmodtime},
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
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime},
		&testFile{name: "file2.txt", content: "klmnopqrstuvwxyz", modtime: fmodtime},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{name: "file3.txt", content: "abcdefghij1", modtime: fmodtime},
		&testFile{name: "file4.txt", content: "klmnopqrstuvwxyz2", modtime: fmodtime},
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
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime},
		&testFile{name: "file2.txt", content: "klmnopqrstuvwxyz", modtime: fmodtime},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime2,
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime},
		&testFile{name: "file2.txt", content: "klmnopqrstuvwxyz", modtime: fmodtime},
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
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime1},
	)
	dir2 := createTestDirectory(
		"testDir2",
		dmodtime,
		&testFile{name: "file1.txt", content: "abcdefghij", modtime: fmodtime2},
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
	return &testDirectory{name: name, files: files, modtime: modtime}
}
