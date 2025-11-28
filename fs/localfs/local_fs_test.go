package localfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	if isWindows {
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
		"foo":      {"", "foo"},
		"/":        {"/", ""},
		"/tmp":     {"/", "tmp"},
		"/tmp/":    {"/tmp/", ""},
		"/tmp/foo": {"/tmp/", "foo"},
	}

	if isWindows {
		cases[`c:/`] = pair{`c:/`, ``}
		cases[`c:\`] = pair{`c:\`, ``}
		cases[`c:/temp`] = pair{`c:/`, `temp`}
		cases[`c:\temp`] = pair{`c:\`, `temp`}
		cases[`c:/temp/orary`] = pair{`c:/temp/`, `orary`}
		cases[`c:\temp\orary`] = pair{`c:\temp\`, `orary`}
		cases[`c:/temp\orary`] = pair{`c:/temp\`, `orary`}
		cases[`c:\temp/orary`] = pair{`c:\temp/`, `orary`}
		cases[`\\server\path`] = pair{`\\server\`, `path`}
		cases[`\\server\path\`] = pair{`\\server\path\`, ``}
		cases[`\\server\path\subdir`] = pair{`\\server\path\`, `subdir`}
	}

	for input, want := range cases {
		basename, prefix := splitDirPrefix(input)
		require.Equal(t, want.basename, basename, input)
		require.Equal(t, want.prefix, prefix, input)
	}
}

// getOptionsFromEntry extracts the options pointer from an fs.Entry by type assertion.
// Returns nil if the entry doesn't have options or if type assertion fails.
func getOptionsFromEntry(entry fs.Entry) *Options {
	switch e := entry.(type) {
	case *filesystemDirectory:
		return e.options
	case *filesystemFile:
		return e.options
	case *filesystemSymlink:
		return e.options
	case *filesystemErrorEntry:
		return e.options
	default:
		return nil
	}
}

func TestOptionsPassedToChildEntries(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	// Create a test directory structure
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "file1.txt"), []byte{1, 2, 3}, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "file2.txt"), []byte{4, 5, 6}, 0o777))
	subdir := filepath.Join(tmp, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(subdir, "subfile.txt"), []byte{7, 8, 9}, 0o777))

	// Create custom options
	customOptions := &Options{
		IgnoreUnreadableDirEntries: true,
	}

	// Create directory with custom options
	dir, err := DirectoryWithOptions(tmp, customOptions)
	require.NoError(t, err)

	// Verify the directory itself has the correct options
	dirOptions := getOptionsFromEntry(dir)
	require.NotNil(t, dirOptions, "directory should have options")
	require.Equal(t, customOptions, dirOptions, "directory should have the same options pointer")
	require.True(t, dirOptions.IgnoreUnreadableDirEntries, "directory options should match")

	// Test that options are passed to children via Child()
	childFile, err := dir.Child(ctx, "file1.txt")
	require.NoError(t, err)

	childOptions := getOptionsFromEntry(childFile)
	require.NotNil(t, childOptions, "child file should have options")
	require.Equal(t, customOptions, childOptions, "child file should have the same options pointer")

	// Test that options are passed to subdirectories
	childDir, err := dir.Child(ctx, "subdir")
	require.NoError(t, err)

	subdirOptions := getOptionsFromEntry(childDir)
	require.NotNil(t, subdirOptions, "subdirectory should have options")
	require.Equal(t, customOptions, subdirOptions, "subdirectory should have the same options pointer")

	// Test that options are passed to nested children
	subdirEntry, ok := childDir.(fs.Directory)
	require.True(t, ok, "child directory should be a directory")

	nestedFile, err := subdirEntry.Child(ctx, "subfile.txt")
	require.NoError(t, err)

	nestedOptions := getOptionsFromEntry(nestedFile)
	require.NotNil(t, nestedOptions, "nested file should have options")
	require.Equal(t, customOptions, nestedOptions, "nested file should have the same options pointer")
}

func TestOptionsPassedThroughIteration(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	// Create a test directory structure
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "file1.txt"), []byte{1, 2, 3}, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "file2.txt"), []byte{4, 5, 6}, 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(tmp, "subdir"), 0o777))

	// Create custom options
	customOptions := &Options{
		IgnoreUnreadableDirEntries: true,
	}

	// Create directory with custom options
	dir, err := DirectoryWithOptions(tmp, customOptions)
	require.NoError(t, err)

	// Iterate through entries and verify all have the same options pointer
	iter, err := dir.Iterate(ctx)
	require.NoError(t, err)

	defer iter.Close()

	entryCount := 0
	for {
		entry, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("iteration error: %v", err)
		}

		if entry == nil {
			break
		}

		entryCount++
		entryOptions := getOptionsFromEntry(entry)
		require.NotNil(t, entryOptions, "entry %s should have options", entry.Name())
		require.Equal(t, customOptions, entryOptions, "entry %s should have the same options pointer", entry.Name())
	}

	require.Equal(t, 3, entryCount, "should have found 3 entries")
}

func TestOptionsPassedThroughSymlinkResolution(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	// Create a target file
	targetFile := filepath.Join(tmp, "target.txt")
	require.NoError(t, os.WriteFile(targetFile, []byte{1, 2, 3}, 0o777))

	// Create a symlink
	symlinkPath := filepath.Join(tmp, "link")
	require.NoError(t, os.Symlink(targetFile, symlinkPath))

	// Create custom options
	customOptions := &Options{
		IgnoreUnreadableDirEntries: true,
	}

	// Create symlink entry with custom options
	symlinkEntry, err := NewEntryWithOptions(symlinkPath, customOptions)
	require.NoError(t, err)

	// Verify the symlink has the correct options
	symlinkOptions := getOptionsFromEntry(symlinkEntry)
	require.NotNil(t, symlinkOptions, "symlink should have options")
	require.Equal(t, customOptions, symlinkOptions, "symlink should have the same options pointer")

	// Resolve the symlink and verify the resolved entry has the same options
	symlink, ok := symlinkEntry.(fs.Symlink)
	require.True(t, ok, "entry should be a symlink")

	resolved, err := symlink.Resolve(ctx)
	require.NoError(t, err)

	resolvedOptions := getOptionsFromEntry(resolved)
	require.NotNil(t, resolvedOptions, "resolved entry should have options")
	require.Equal(t, customOptions, resolvedOptions, "resolved entry should have the same options pointer")
}

func TestOptionsPassedToNewEntry(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	// Create a file
	filePath := filepath.Join(tmp, "testfile.txt")
	require.NoError(t, os.WriteFile(filePath, []byte{1, 2, 3}, 0o777))

	// Create custom options
	customOptions := &Options{
		IgnoreUnreadableDirEntries: true,
	}

	// Create entry with custom options
	entry, err := NewEntryWithOptions(filePath, customOptions)
	require.NoError(t, err)

	// Verify the entry has the correct options
	entryOptions := getOptionsFromEntry(entry)
	require.NotNil(t, entryOptions, "entry should have options")
	require.Equal(t, customOptions, entryOptions, "entry should have the same options pointer")
}

func TestOptionsPassedToNestedDirectories(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	// Create nested directory structure
	level1 := filepath.Join(tmp, "level1")
	level2 := filepath.Join(level1, "level2")
	level3 := filepath.Join(level2, "level3")

	require.NoError(t, os.MkdirAll(level3, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(level3, "file.txt"), []byte{1, 2, 3}, 0o777))

	// Create custom options
	customOptions := &Options{
		IgnoreUnreadableDirEntries: true,
	}

	// Create root directory with custom options
	rootDir, err := DirectoryWithOptions(tmp, customOptions)
	require.NoError(t, err)

	// Navigate through nested directories and verify options are passed
	currentDir := rootDir
	levels := []string{"level1", "level2", "level3"}

	for _, level := range levels {
		child, err := currentDir.Child(ctx, level)
		require.NoError(t, err)

		childOptions := getOptionsFromEntry(child)
		require.NotNil(t, childOptions, "directory %s should have options", level)
		require.Equal(t, customOptions, childOptions, "directory %s should have the same options pointer", level)

		var ok bool

		currentDir, ok = child.(fs.Directory)
		require.True(t, ok, "child should be a directory")
	}

	// Verify the file in the deepest directory has the same options
	file, err := currentDir.Child(ctx, "file.txt")
	require.NoError(t, err)

	fileOptions := getOptionsFromEntry(file)
	require.NotNil(t, fileOptions, "file should have options")
	require.Equal(t, customOptions, fileOptions, "file should have the same options pointer")
}

func TestDefaultOptionsUsedByDefault(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	// Create a file
	filePath := filepath.Join(tmp, "testfile.txt")
	require.NoError(t, os.WriteFile(filePath, []byte{1, 2, 3}, 0o777))

	// Use default NewEntry (should use DefaultOptions)
	entry, err := NewEntry(filePath)
	require.NoError(t, err)

	// Verify the entry has DefaultOptions
	entryOptions := getOptionsFromEntry(entry)
	require.NotNil(t, entryOptions, "entry should have options")
	require.Equal(t, DefaultOptions, entryOptions, "entry should have DefaultOptions pointer")
}

func TestDifferentOptionsInstances(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	// Create two different files
	filePath1 := filepath.Join(tmp, "testfile1.txt")
	filePath2 := filepath.Join(tmp, "testfile2.txt")

	require.NoError(t, os.WriteFile(filePath1, []byte{1, 2, 3}, 0o777))
	require.NoError(t, os.WriteFile(filePath2, []byte{4, 5, 6}, 0o777))

	// Create two different options instances with same values
	options1 := &Options{IgnoreUnreadableDirEntries: true}
	options2 := &Options{IgnoreUnreadableDirEntries: false}

	// Create entries with different options instances
	entry1, err := NewEntryWithOptions(filePath1, options1)
	require.NoError(t, err)

	entry2, err := NewEntryWithOptions(filePath2, options2)
	require.NoError(t, err)

	// Verify they have the correct options pointers
	entry1Options := getOptionsFromEntry(entry1)
	entry2Options := getOptionsFromEntry(entry2)

	require.NotNil(t, entry1Options)
	require.NotNil(t, entry2Options)
	require.Equal(t, options1, entry1Options, "entry1 should have options1 pointer")
	require.Equal(t, options2, entry2Options, "entry2 should have options2 pointer")
	require.NotEqual(t, entry1Options, entry2Options, "entries should have different options pointers")
	require.True(t, entry1Options.IgnoreUnreadableDirEntries, "entry1 options should have IgnoreUnreadableDirEntries=true")
	require.False(t, entry2Options.IgnoreUnreadableDirEntries, "entry2 options should have IgnoreUnreadableDirEntries=false")
}
