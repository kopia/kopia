package localfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

type fileEnt struct {
	size   int64
	isFile bool
}

//nolint:gocyclo
func TestFiles(t *testing.T) {
	ctx := testlogging.Context(t)

	var err error

	tmp := testutil.TempDirectory(t)

	var dir fs.Directory

	// Try listing directory that does not exist.
	_, err = Directory(fmt.Sprintf("/no-such-dir-%v", clock.Now().Nanosecond()))
	if err == nil {
		t.Errorf("expected error when dir directory that does not exist.")
	}

	// Now list an empty directory that does exist.
	dir, err = Directory(tmp)
	if err != nil {
		t.Errorf("error when dir empty directory: %v", err)
	}

	entries, err := fs.GetAllEntries(ctx, dir)
	if err != nil {
		t.Errorf("error gettind dir Entries: %v", err)
	}

	if len(entries) > 0 {
		t.Errorf("expected empty directory, got %v", dir)
	}

	// Now list a directory with 3 files.
	assertNoError(t, os.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0o777))
	assertNoError(t, os.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0o777))
	assertNoError(t, os.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0o777))

	assertNoError(t, os.Mkdir(filepath.Join(tmp, "z"), 0o777))
	assertNoError(t, os.Mkdir(filepath.Join(tmp, "y"), 0o777))

	expected := map[string]fileEnt{
		"f1": {
			size:   5,
			isFile: true,
		},
		"f2": {
			size:   4,
			isFile: true,
		},
		"f3": {
			size:   3,
			isFile: true,
		},
		"y": {
			size:   0,
			isFile: false,
		},
		"z": {
			size:   0,
			isFile: false,
		},
	}

	dir, err = Directory(tmp)
	if err != nil {
		t.Errorf("error when dir directory with files: %v", err)
	}

	entries, err = fs.GetAllEntries(ctx, dir)
	if err != nil {
		t.Errorf("error gettind dir Entries: %v", err)
	}

	goodCount := 0

	for _, found := range entries {
		wanted, ok := expected[found.Name()]
		if !ok {
			continue
		}

		if found.Size() != wanted.size {
			continue
		}

		if wanted.isFile {
			if !found.Mode().IsRegular() {
				continue
			}
		} else {
			if !found.Mode().IsDir() {
				continue
			}
		}

		goodCount++
	}

	if goodCount != 5 {
		t.Errorf("invalid dir data: %v good entries", goodCount)

		for i, e := range entries {
			t.Logf("e[%v] = %v %v %v", i, e.Name(), e.Size(), e.Mode())
		}
	}

	verifyChild(t, dir)
}

func TestIterate1000(t *testing.T) {
	testIterate(t, 1000)
}

func TestIterate10(t *testing.T) {
	testIterate(t, 10)
}

func TestIterateNonExistent(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	dir, err := Directory(tmp)
	require.NoError(t, err)
	os.Remove(tmp)

	ctx := testlogging.Context(t)

	require.ErrorIs(t, dir.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		t.Fatal("this won't be invoked")
		return nil
	}), os.ErrNotExist)
}

//nolint:thelper
func testIterate(t *testing.T, nFiles int) {
	tmp := testutil.TempDirectory(t)

	for i := 0; i < nFiles; i++ {
		assertNoError(t, os.WriteFile(filepath.Join(tmp, fmt.Sprintf("f%v", i)), []byte{1, 2, 3}, 0o777))
	}

	dir, err := Directory(tmp)
	require.NoError(t, err)

	ctx := testlogging.Context(t)

	names := map[string]int64{}

	require.NoError(t, dir.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		names[e.Name()] = e.Size()
		return nil
	}))

	require.Len(t, names, nFiles)

	errTest := errors.New("test error")

	cnt := 0

	require.ErrorIs(t, dir.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		cnt++

		if cnt == nFiles/10 {
			return errTest
		}

		return nil
	}), errTest)

	cnt = 0

	require.ErrorIs(t, dir.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		cnt++

		if cnt == nFiles-1 {
			return errTest
		}

		return nil
	}), errTest)
}

func verifyChild(t *testing.T, dir fs.Directory) {
	t.Helper()

	ctx := testlogging.Context(t)

	child, err := dir.Child(ctx, "f3")
	if err != nil {
		t.Errorf("child error: %v", err)
	}

	if _, err = dir.Child(ctx, "f4"); !errors.Is(err, fs.ErrEntryNotFound) {
		t.Errorf("unexpected child error: %v", err)
	}

	if got, want := child.Name(), "f3"; got != want {
		t.Errorf("unexpected child name: %v, want %v", got, want)
	}

	if got, want := child.Size(), int64(3); got != want {
		t.Errorf("unexpected child size: %v, want %v", got, want)
	}

	if _, err = fs.IterateEntriesAndFindChild(ctx, dir, "f4"); !errors.Is(err, fs.ErrEntryNotFound) {
		t.Errorf("unexpected child error: %v", err)
	}

	// read child again, this time using IterateEntriesAndFindChild
	child2, err := fs.IterateEntriesAndFindChild(ctx, dir, "f3")
	if err != nil {
		t.Errorf("child2 error: %v", err)
	}

	if got, want := child2.Name(), "f3"; got != want {
		t.Errorf("unexpected child2 name: %v, want %v", got, want)
	}

	if got, want := child2.Size(), int64(3); got != want {
		t.Errorf("unexpected child2 size: %v, want %v", got, want)
	}
}

func TestLocalFilesystemPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	testDir := testutil.TempDirectory(t)

	cases := map[string]string{
		"/":           "/",
		testDir:       testDir,
		testDir + "/": testDir,
	}

	for input, want := range cases {
		ent, err := NewEntry(input)
		require.NoError(t, err)

		dir, ok := ent.(fs.Directory)
		require.True(t, ok, input)

		require.Equal(t, want, dir.LocalFilesystemPath())
	}
}

func TestDirPrefix(t *testing.T) {
	cases := map[string]string{
		"foo":      "",
		"/":        "/",
		"/tmp":     "/",
		"/tmp/":    "/tmp/",
		"/tmp/foo": "/tmp/",
	}

	if runtime.GOOS == "windows" {
		cases["c:/"] = "c:/"
		cases["c:\\"] = "c:\\"
		cases["c:/temp"] = "c:/"
		cases["c:\\temp"] = "c:\\"
		cases["c:/temp/orary"] = "c:/temp/"
		cases["c:\\temp\\orary"] = "c:\\temp\\"
		cases["c:/temp\\orary"] = "c:/temp\\"
		cases["c:\\temp/orary"] = "c:\\temp/"
		cases["\\\\server\\path"] = "\\\\server\\"
		cases["\\\\server\\path\\subdir"] = "\\\\server\\path\\"
	}

	for input, want := range cases {
		require.Equal(t, want, dirPrefix(input), input)
	}
}

func TestSymlink(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	fileName := "file"
	dirName := "dir"
	fileLinkName := "fileLink"
	dirLinkName := "dirLink"

	filePath := filepath.Join(tmp, fileName)
	dirPath := filepath.Join(tmp, dirName)
	fileLinkPath := filepath.Join(tmp, fileLinkName)
	dirLinkPath := filepath.Join(tmp, dirLinkName)

	assertNoError(t, os.WriteFile(filePath, []byte{1, 2}, 0o777))
	assertNoError(t, os.Mkdir(dirPath, 0o777))
	assertNoError(t, os.Symlink(filePath, fileLinkPath))
	assertNoError(t, os.Symlink(dirPath, dirLinkPath))

	dir, err := Directory(tmp)
	require.NoError(t, err)
	ctx := testlogging.Context(t)

	file, err := dir.Child(ctx, fileLinkName)
	if err != nil {
		t.Errorf("Child error: %v", err)
	}

	f, ok := file.(fs.Symlink)
	if !ok {
		t.Errorf("symlink is not detected as a symlink")
	}

	path, err := f.Readlink(ctx)
	if err != nil {
		t.Errorf("Unable to read link: %v", err)
	}

	require.Equal(t, path, filePath)

	obj, err := f.Resolve(ctx)
	if err != nil {
		t.Errorf("Resolve error: %v", err)
	}

	if _, ok = obj.(fs.File); !ok {
		t.Error("Symlink is not pointing to a file")
	}

	file, err = dir.Child(ctx, dirLinkName)
	if err != nil {
		t.Errorf("Child error: %v", err)
	}

	f, ok = file.(fs.Symlink)
	if !ok {
		t.Errorf("symlink is not detected as a symlink")
	}

	path, err = f.Readlink(ctx)
	if err != nil {
		t.Errorf("Unable to read link: %v", err)
	}

	require.Equal(t, path, dirPath)

	obj, err = f.Resolve(ctx)
	if err != nil {
		t.Errorf("Resolve error: %v", err)
	}

	if _, ok = obj.(fs.Directory); !ok {
		t.Error("Symlink is not pointing to a directory")
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}
