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

func TestSymlink(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	fn := filepath.Join(tmp, "target")
	absLink := filepath.Join(tmp, "abslink")
	relLink := filepath.Join(tmp, "rellink")

	require.NoError(t, os.WriteFile(fn, []byte{1, 2, 3}, 0o777))
	require.NoError(t, os.Symlink(fn, absLink))
	require.NoError(t, os.Symlink("./target", relLink))

	verifyLink(t, absLink, fn)
	verifyLink(t, relLink, fn)
}

func verifyLink(t *testing.T, path, expected string) {
	t.Helper()

	ctx := testlogging.Context(t)

	entry, err := NewEntry(path)
	require.NoError(t, err)

	link, ok := entry.(fs.Symlink)
	require.True(t, ok, "entry is not a symlink:", entry)

	target, err := link.Resolve(ctx)
	require.NoError(t, err)

	f, ok := target.(fs.File)
	require.True(t, ok, "link does not point to a file:", path)

	// Canonicalize paths (for example, on MacOS /var points to /private/var)
	// EvalSymlinks calls "Clean" on the result
	got, err := filepath.EvalSymlinks(f.LocalFilesystemPath())
	require.NoError(t, err)

	want, err := filepath.EvalSymlinks(expected)
	require.NoError(t, err)

	require.Equal(t, want, got)
}

//nolint:gocyclo
func TestFiles(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	// Try listing directory that does not exist.
	_, err := Directory(fmt.Sprintf("/no-such-dir-%v", clock.Now().Nanosecond()))
	require.Error(t, err, "expected error when dir directory that does not exist.")

	// Now list an empty directory that does exist.
	dir, err := Directory(tmp)
	require.NoError(t, err, "error when dir empty directory")

	entries, err := fs.GetAllEntries(ctx, dir)
	require.NoError(t, err, "error gettind dir Entries")
	require.Empty(t, entries, "expected empty directory")

	// Now list a directory with 3 files.
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(tmp, "z"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(tmp, "y"), 0o777))

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
	require.NoError(t, err, "error when dir directory with files")

	entries, err = fs.GetAllEntries(ctx, dir)
	require.NoError(t, err, "error gettind dir Entries")

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

	require.ErrorIs(t, fs.IterateEntries(ctx, dir, func(ctx context.Context, e fs.Entry) error {
		t.Fatal("this won't be invoked")
		return nil
	}), os.ErrNotExist)
}

//nolint:thelper
func testIterate(t *testing.T, nFiles int) {
	tmp := testutil.TempDirectory(t)

	for i := range nFiles {
		require.NoError(t, os.WriteFile(filepath.Join(tmp, fmt.Sprintf("f%v", i)), []byte{1, 2, 3}, 0o777))
	}

	dir, err := Directory(tmp)
	require.NoError(t, err)

	ctx := testlogging.Context(t)

	names := map[string]int64{}

	require.NoError(t, fs.IterateEntries(ctx, dir, func(ctx context.Context, e fs.Entry) error {
		names[e.Name()] = e.Size()
		return nil
	}))

	require.Len(t, names, nFiles)

	errTest := errors.New("test error")

	cnt := 0

	require.ErrorIs(t, fs.IterateEntries(ctx, dir, func(ctx context.Context, e fs.Entry) error {
		cnt++

		if cnt == nFiles/10 {
			return errTest
		}

		return nil
	}), errTest)

	cnt = 0

	require.ErrorIs(t, fs.IterateEntries(ctx, dir, func(ctx context.Context, e fs.Entry) error {
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

	require.NoError(t, err, "child error")
	require.Equal(t, "f3", child.Name(), "unexpected child name")
	require.Equal(t, int64(3), child.Size(), "unexpected child size")

	_, err = dir.Child(ctx, "f4")
	require.ErrorIs(t, err, fs.ErrEntryNotFound, "unexpected child error")

	_, err = fs.IterateEntriesAndFindChild(ctx, dir, "f4")
	require.ErrorIs(t, err, fs.ErrEntryNotFound, "unexpected child error")

	// read child again, this time using IterateEntriesAndFindChild
	child2, err := fs.IterateEntriesAndFindChild(ctx, dir, "f3")
	require.NoError(t, err, "child2 error")
	require.Equal(t, "f3", child2.Name(), "unexpected child name")
	require.Equal(t, int64(3), child2.Size(), "unexpected child size")
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

func TestSplitDirPrefix(t *testing.T) {
	type pair struct {
		prefix   string
		basename string
	}

	cases := map[string]pair{
		"foo":      pair{"", "foo"},
		"/":        pair{"/", ""},
		"/tmp":     pair{"/", "tmp"},
		"/tmp/":    pair{"/tmp/", ""},
		"/tmp/foo": pair{"/tmp/", "foo"},
	}

	if runtime.GOOS == "windows" {
		cases["c:/"] = pair{"c:/", ""}
		cases["c:\\"] = pair{"c:\\", ""}
		cases["c:/temp"] = pair{"c:/", "temp"}
		cases["c:\\temp"] = pair{"c:\\", "temp"}
		cases["c:/temp/orary"] = pair{"c:/temp/", "orary"}
		cases["c:\\temp\\orary"] = pair{"c:\\temp\\", "orary"}
		cases["c:/temp\\orary"] = pair{"c:/temp\\", "orary"}
		cases["c:\\temp/orary"] = pair{"c:\\temp/", "orary"}
		cases["\\\\server\\path"] = pair{"\\\\server\\", "path"}
		cases["\\\\server\\path\\"] = pair{"\\\\server\\path\\", ""}
		cases["\\\\server\\path\\subdir"] = pair{"\\\\server\\path\\", "subdir"}
	}

	for input, want := range cases {
		basename, prefix := splitDirPrefix(input)
		require.Equal(t, want.basename, basename, input)
		require.Equal(t, want.prefix, prefix, input)
	}
}
