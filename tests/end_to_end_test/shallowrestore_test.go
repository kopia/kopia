package endtoend_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

func TestShallowrestore(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	e.RunAndExpectSuccess(t, "snapshot", "create", source)
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	if got, want := len(sources), 1; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}

	snapID := sources[0].Snapshots[0].SnapshotID
	rootID := sources[0].Snapshots[0].ObjectID

	rdc := &repoDirEntryCache{
		e:           e,
		rootid:      rootID,
		reporootdir: source,
	}

	for depth := 1; depth < 5; depth++ {
		shallowrestoredir := filepath.Join(t.TempDir(), "shallowrestoredir")
		shallowarg := fmt.Sprintf("--shallow=%d", depth-1)
		e.RunAndExpectSuccess(t, "restore", shallowarg, snapID, shallowrestoredir)
		compareShallowToOriginalDir(t, rdc, source, shallowrestoredir, depth)
	}
}

func TestShallowrestoreWithMinSize(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.Mkdir(source, 0o755))

	big := filepath.Join(source, "big")
	testdirtree.MustCreateRandomFile(t, big, testdirtree.DirectoryTreeOptions{
		MinFileSize: 1000,
	}, (*testdirtree.DirectoryTreeCounters)(nil))

	little := filepath.Join(source, "little")
	testdirtree.MustCreateRandomFile(t, little, testdirtree.DirectoryTreeOptions{
		MaxFileSize: 1000,
	}, (*testdirtree.DirectoryTreeCounters)(nil))

	e.RunAndExpectSuccess(t, "snapshot", "create", source)
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	if got, want := len(sources), 1; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}

	snapID := sources[0].Snapshots[0].SnapshotID
	shallowrestoredir := filepath.Join(t.TempDir(), "shallowrestoredir")

	e.RunAndExpectSuccess(t, "restore", "--shallow=0", "--shallow-minsize=1000", snapID, shallowrestoredir)

	little = filepath.Join(shallowrestoredir, "little")
	big = filepath.Join(shallowrestoredir, "big")

	require.FileExists(t, little)
	require.NoFileExists(t, little+localfs.ShallowEntrySuffix)
	require.FileExists(t, big+localfs.ShallowEntrySuffix)
	require.NoFileExists(t, big)
}

func TestShallowFullCycle(t *testing.T) {
	t.Parallel()
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	// Some of the different mutations require a directory to exist. Let's make
	// certain that we have one.
	dirpath := filepath.Join(source, "atleastonedir")
	fpath := filepath.Join(source, "atleastonedir", "nestedfile")

	require.NoError(t, os.Mkdir(dirpath, 0o755))
	testdirtree.MustCreateRandomFile(t, fpath, testdirtree.DirectoryTreeOptions{}, (*testdirtree.DirectoryTreeCounters)(nil))

	// Directories with very long names are not representable in
	// shallow restores. So make one to show that it works.
	// Restores (and shallow restores) with overly-long names will both fail.
	dirpathlong := filepath.Join(source, makeLongName('d'))
	fpathinlong := filepath.Join(dirpathlong, "nestedfile")

	require.NoError(t, os.Mkdir(dirpathlong, 0o755))
	testdirtree.MustCreateRandomFile(t, atomicfile.MaybePrefixLongFilenameOnWindows(fpathinlong), testdirtree.DirectoryTreeOptions{}, (*testdirtree.DirectoryTreeCounters)(nil))

	e.RunAndExpectSuccess(t, "snapshot", "create", source)
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	originalsnapshotid := sources[0].Snapshots[0].SnapshotID
	rootID := sources[0].Snapshots[0].ObjectID

	// for all of the directory combos.
	for _, mutate := range []filesystemmutator{
		doNothing,
		addOneFile,
		moveFile,
		deepenSubtreeFile,
		deepenSubtreeDirectory,
		removeEntry,
		moveDirectory,
		deepenOneSubtreeLevel,
		addForeignSnapshotTree,
	} {
		// Make a base copy of the test directory that I will then mutate via
		// manipulations of the real and shallow restore.
		mutatedoriginal := t.TempDir()
		e.RunAndExpectSuccess(t, "restore", originalsnapshotid, mutatedoriginal)

		// Make sure that the mutatedoriginal and the source are really the same.
		require.NoError(t, os.Chmod(mutatedoriginal, 0o700))
		compareDirs(t, source, mutatedoriginal)

		// Make a shallowrestore of the test directory.
		shallow := t.TempDir()
		e.RunAndExpectSuccess(t, "restore", "--shallow=0", originalsnapshotid, shallow)

		// Apply the change to both the shallow restore and the original. (Single function to
		// share state.)
		mutate(&mutatorArgs{
			t:                  t,
			e:                  e,
			original:           mutatedoriginal,
			shallow:            shallow,
			originalsnapshotid: originalsnapshotid,
			rdc: &repoDirEntryCache{
				e:           e,
				rootid:      rootID,
				reporootdir: mutatedoriginal,
			},
		})

		// Take a snapshot of the mutated shallow tree.
		e.RunAndExpectSuccess(t, "snapshot", "create", shallow)

		// Get snapshot id for the shallow tree's snapshot.
		shallowsnapshotid := getSnapID(t, clitestutil.ListSnapshotsAndExpectSuccess(t, e), shallow)

		full := t.TempDir()
		e.RunAndExpectSuccess(t, "restore", shallowsnapshotid, full)

		// Force permissions to be reset so that the recursive directory comparison works
		// per comment in restore_test.go
		require.NoError(t, os.Chmod(mutatedoriginal, 0o700))
		require.NoError(t, os.Chmod(full, 0o700))

		compareDirs(t, mutatedoriginal, full)
	}
}

// addOneFile mutates a test hierarchy by adding a single randomly
// created file at the top level.
func addOneFile(m *mutatorArgs) {
	origpath := filepath.Join(m.original, "testfile")
	shallowpath := filepath.Join(m.shallow, "testfile")
	testdirtree.MustCreateRandomFile(m.t, origpath, testdirtree.DirectoryTreeOptions{}, (*testdirtree.DirectoryTreeCounters)(nil))
	require.NoError(m.t, os.Link(origpath, shallowpath))
}

// doNothing is a nop mutation of the provided test file tree.
func doNothing(_ *mutatorArgs) {
}

// mplfow makes atomicfile.MaybePrefixLongFilenameOnWindows easier to type.
func mplfow(fname string) string {
	return atomicfile.MaybePrefixLongFilenameOnWindows(fname)
}

// moveDirectory moves a directory from one location to another (in the
// shallow and original trees.
func moveDirectory(m *mutatorArgs) {
	m.t.Log("moveDirectory", "original: ", m.original, "shallow: ", m.shallow)

	dirinshallow, _ := findFileDir(m.t, m.shallow)
	if dirinshallow == "" {
		m.t.Errorf("can't run moveDirectory, no directory")
		return
	}

	// 2. create a new directory in shallow and original
	relpath := strings.TrimPrefix(dirinshallow, m.shallow)
	relpathinreal := localfs.TrimShallowSuffix(relpath)
	m.t.Log("moveDirectory", "relpath:", relpath)
	newshallowdir := filepath.Join(m.shallow, "newdir")
	neworiginaldir := filepath.Join(m.original, "newdir")
	m.t.Log("moveDirectory", "newshallowdir:", newshallowdir, "neworiginaldir:", neworiginaldir, "relpathinreal:", relpathinreal)

	require.NoError(m.t, os.Mkdir(newshallowdir, 0o755))
	require.NoError(m.t, os.Mkdir(neworiginaldir, 0o755))

	// 3. move shallow dir into new dir, original dir into new dir
	require.NoError(m.t, os.Rename(mplfow(dirinshallow), mplfow(filepath.Join(newshallowdir, relpath))))
	require.NoError(m.t, os.Rename(mplfow(filepath.Join(m.original, relpathinreal)), mplfow(filepath.Join(neworiginaldir, relpathinreal))))

	// 4. fix new directory timestamp to be the same
	fi, err := os.Stat(mplfow(newshallowdir))
	require.NoError(m.t, err)
	require.NoError(m.t, os.Chtimes(neworiginaldir, fi.ModTime(), fi.ModTime()))
	require.NoError(m.t, os.Chtimes(newshallowdir, fi.ModTime(), fi.ModTime()))
}

// moveFile moves a file from one location to another (in the shallow and original trees).
func moveFile(m *mutatorArgs) {
	m.t.Log("moveFile", "original: ", m.original, "shallow: ", m.shallow)

	_, fileinshallow := findFileDir(m.t, m.shallow)
	if fileinshallow == "" {
		m.t.Errorf("can't run moveDirectory, no directory")
		return
	}

	// 2. create a new directory in shallow and original
	relpath := strings.TrimPrefix(fileinshallow, m.shallow)
	m.t.Log("moveDirectory", "relpath:", relpath)
	newshallowdir := filepath.Join(m.shallow, "newdir")
	neworiginaldir := filepath.Join(m.original, "newdir")
	m.t.Log("moveDirectory", "newshallowdir:", newshallowdir, "neworiginaldir:", neworiginaldir)

	require.NoError(m.t, os.Mkdir(newshallowdir, 0o755))
	require.NoError(m.t, os.Mkdir(neworiginaldir, 0o755))

	// 3. move shallow file into new dir, original dir into new dir
	require.NoError(m.t, os.Rename(mplfow(fileinshallow), mplfow(filepath.Join(newshallowdir, relpath))))
	require.NoError(m.t, os.Rename(mplfow(filepath.Join(m.original, localfs.TrimShallowSuffix(relpath))), mplfow(filepath.Join(neworiginaldir, localfs.TrimShallowSuffix(relpath)))))

	// 4. fix new directory timestamp to be the same
	fi, err := os.Stat(mplfow(newshallowdir))
	require.NoError(m.t, err)
	require.NoError(m.t, os.Chtimes(neworiginaldir, fi.ModTime(), fi.ModTime()))
	require.NoError(m.t, os.Chtimes(newshallowdir, fi.ModTime(), fi.ModTime()))
}

// deepenSubtreeDirectory reifies a shallow directory entry with its actual
// contents (an entire directory hierarchy.)
func deepenSubtreeDirectory(m *mutatorArgs) {
	// 1. Find a directory.
	dirinshallow, _ := findFileDir(m.t, m.shallow)
	if dirinshallow == "" {
		m.t.Errorf("can't run deepenSubtreeDirectory, no directory")
		return
	}

	// 2. do a full restore of it.
	m.e.RunAndExpectSuccess(m.t, "restore", "--shallow=1000", dirinshallow)

	// 3. Original shouldn't require any changes as the entire tree should
	// be there.
} //nolint:wsl

// deepenSubtreeFile reifies a shallow file entry with its actual contents.
func deepenSubtreeFile(m *mutatorArgs) {
	// 1. find a shallow file entry in the shallow restore
	_, fileinshallow := findFileDir(m.t, m.shallow)
	if fileinshallow == "" {
		m.t.Errorf("can't run deepenSubtreeFile, no file")
		return
	}

	// 2. do a full restore of it.
	m.e.RunAndExpectSuccess(m.t, "restore", "--shallow=1000", fileinshallow)

	// 3. Original shouldn't require any changes.
} //nolint:wsl

// deepenOneSubtreeLevel reifies a shallow directory entry with one level
// of reification. In particular: given a path into a shallow restored
// tree, we restore a single shallow directory and the directory should
// become a real (mutable) directory containing shallow entries.
// TODO(rjk): generalize the testing of the shallow restoration
// validation to make sure that the restored directory of this form has
// the correct form.
func deepenOneSubtreeLevel(m *mutatorArgs) {
	// 1. find a (shallow) directory
	dirinshallow, fileinshallow := findFileDir(m.t, m.shallow)
	if dirinshallow == "" {
		m.t.Errorf("can't run deepenOneSubtreeLevel, no shallow directory")
		return
	}

	relpath := strings.TrimPrefix(dirinshallow, m.shallow)
	m.t.Log("relpath", relpath)

	// 2. shallow restore it into the shallow tree
	m.e.RunAndExpectSuccess(m.t, "restore", dirinshallow, fileinshallow)

	// 2.5 verify that the restored subtree is correctly real and shallow
	origpath := filepath.Join(m.original, relpath)

	// depth is 1 because we've expanded one level down.
	compareShallowToOriginalDir(m.t, m.rdc, localfs.TrimShallowSuffix(origpath), localfs.TrimShallowSuffix(dirinshallow), 1)
	compareShallowToOriginalDir(m.t, m.rdc, localfs.TrimShallowSuffix(origpath), localfs.TrimShallowSuffix(fileinshallow), 1)

	// 3. Original shouldn't require any changes.
} //nolint:wsl

// removeEntry tests that we can remove both directory and file shallow
// placeholders.
func removeEntry(m *mutatorArgs) {
	// 1. find a (shallow) file
	dirinshallow, fileinshallow := findFileDir(m.t, m.shallow)
	if fileinshallow == "" {
		m.t.Errorf("can't run removeFile, no file")
		return
	}

	filerelpath := strings.TrimPrefix(localfs.TrimShallowSuffix(fileinshallow), m.shallow)
	dirrelpath := strings.TrimPrefix(localfs.TrimShallowSuffix(dirinshallow), m.shallow)
	m.t.Log(">> relpath", filerelpath, dirrelpath, fileinshallow, dirinshallow)

	// 2. remove
	require.NoError(m.t, os.Remove(fileinshallow))
	require.NoError(m.t, os.RemoveAll(dirinshallow))

	// 3. remove from full
	fopath := filepath.Join(m.original, filerelpath)
	dopath := filepath.Join(m.original, dirrelpath)
	require.NoError(m.t, os.RemoveAll(fopath))
	require.NoError(m.t, os.RemoveAll(dopath))
}

// addForeignSnapshotTree adds a completely different snapshot to a tree.
func addForeignSnapshotTree(m *mutatorArgs) {
	// 1. make a completely different snapshot of a different tree
	foreign := filepath.Join(m.t.TempDir(), "foreign")
	testdirtree.MustCreateDirectoryTree(m.t, foreign, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})
	m.e.RunAndExpectSuccess(m.t, "snapshot", "create", foreign)
	foreignsnapshotid := getSnapID(m.t, clitestutil.ListSnapshotsAndExpectSuccess(m.t, m.e), foreign)

	foreignshallowdir := filepath.Join(m.shallow, "foreigndir")
	foreignoriginaldir := filepath.Join(m.original, "foreigndir")
	m.t.Log("addForeignRepoTree", "foreignshallowdir:", foreignshallowdir, "foreignoriginaldir:", foreignoriginaldir)

	// 2. shallowrestore it into shallow
	m.e.RunAndExpectSuccess(m.t, "restore", "--shallow=0", foreignsnapshotid, foreignshallowdir)

	// 3. full restore it into deep
	m.e.RunAndExpectSuccess(m.t, "restore", foreignsnapshotid, foreignoriginaldir)

	// 4. make the times match.
	fi, err := os.Stat(foreignshallowdir)
	require.NoError(m.t, err)
	require.NoError(m.t, os.Chtimes(foreignshallowdir, fi.ModTime(), fi.ModTime()))
	require.NoError(m.t, os.Chtimes(foreignoriginaldir, fi.ModTime(), fi.ModTime()))
}

func TestShallowifyTree(t *testing.T) {
	t.Parallel()
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create a snapshot.
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	// Snapshot original tree.
	e.RunAndExpectSuccess(t, "snapshot", "create", source)
	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	originalsnapshotid := sources[0].Snapshots[0].SnapshotID

	// 1. Create a full restore of the tree.
	mutatedoriginal := t.TempDir()
	e.RunAndExpectSuccess(t, "restore", originalsnapshotid, mutatedoriginal)

	// 2. overwrite the tree with a shallow tree. Expected to fail: overwriting is
	// dangerous so causes an error.
	e.RunAndExpectFailure(t, "shallow", "--shallow=0", originalsnapshotid, mutatedoriginal)
}

func TestPlaceholderAndRealFails(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create a snapshot.
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	// At least one directory is required so make one.
	dirpath := filepath.Join(source, "atleastonedir")
	fpath := filepath.Join(source, "atleastonedir", "nestedfile")

	require.NoError(t, os.Mkdir(dirpath, 0o755))

	testdirtree.MustCreateRandomFile(t, fpath, testdirtree.DirectoryTreeOptions{}, (*testdirtree.DirectoryTreeCounters)(nil))

	origdir, origfile := findRealFileDir(t, source)
	if origdir == "" || origfile == "" {
		t.Fatalf("missing paths %q, %q", origdir, origfile)
	}

	// Placeholder file.
	pfpath := origfile + localfs.ShallowEntrySuffix
	phfd, err := os.Create(pfpath)
	require.NoError(t, err)
	require.NoError(t, phfd.Close())
	e.RunAndExpectFailure(t, "snapshot", "create", source)
	require.NoError(t, os.RemoveAll(pfpath))

	// Placeholder dir, no file.
	pfdirpath := origfile + localfs.ShallowEntrySuffix
	require.NoError(t, os.MkdirAll(pfdirpath, os.FileMode(dirMode)))
	e.RunAndExpectFailure(t, "snapshot", "create", source)
	require.NoError(t, os.RemoveAll(pfdirpath))

	// Placeholder dir, and file.
	pfdirfilepath := origfile + dIRPH

	require.NoError(t, os.MkdirAll(pfdirpath, os.FileMode(dirMode)))

	pfdirfd, err := os.Create(pfdirfilepath)
	require.NoError(t, err)
	require.NoError(t, pfdirfd.Close())
	e.RunAndExpectFailure(t, "snapshot", "create", source)
	require.NoError(t, os.RemoveAll(pfdirfilepath))
}

// TestForeignReposCauseErrors detects that shallow placeholders from
// other repositories (i.e. whose object.ID members are not valid
// repository objects.
func TestForeignReposCauseErrors(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create a snapshot.
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	for _, s := range []struct {
		mkdir bool
		de    *snapshot.DirEntry
	}{
		{
			mkdir: true,
			de: &snapshot.DirEntry{
				Name:     "badplaceholder",
				Type:     "d",
				ObjectID: mustParseID(t, "Df0f0"),
			},
		},
		{
			de: &snapshot.DirEntry{
				Name:     "badplaceholder",
				Type:     "f",
				ObjectID: mustParseID(t, "IDf0f0"),
			},
		},
	} {
		spath := filepath.Join(source, "badplaceholder"+localfs.ShallowEntrySuffix)
		depath := spath

		if s.mkdir {
			require.NoError(t, os.MkdirAll(spath, os.FileMode(dirMode)))
			depath = filepath.Join(spath, localfs.ShallowEntrySuffix)
		}

		buffy := &bytes.Buffer{}
		encoder := json.NewEncoder(buffy)
		require.NoError(t, encoder.Encode(s.de))
		require.NoError(t, os.WriteFile(depath, buffy.Bytes(), 0o444))
		e.RunAndExpectFailure(t, "snapshot", "create", source)
		require.NoError(t, os.RemoveAll(spath))
	}
}

// --- Helper routines start here.

const (
	// d1 + kDIRPH is the DirEntry placeholder for original directory d1.
	dIRPH = localfs.ShallowEntrySuffix + string(filepath.Separator) + localfs.ShallowEntrySuffix

	// d1 + kSUBFILE is the DirEntry placeholder for placeholder directory d1.kopia-entry.
	sUBFILE = string(filepath.Separator) + localfs.ShallowEntrySuffix

	dirMode = 0o700
)

// getShallowDirEntry reads the DirEntry in the placeholder associated
// with fpath, fpath.kopia-dir, fpath.kopia-dir/.kopia-dir.
func getShallowDirEntry(t *testing.T, fpath string) *snapshot.DirEntry {
	t.Helper()

	var (
		b   []byte
		err error
	)

	t.Logf("fpath %q", fpath)

	for _, s := range []string{localfs.ShallowEntrySuffix, sUBFILE, dIRPH} {
		p := fpath
		if !strings.HasSuffix(fpath, s) {
			p += s
		}

		b, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}

	require.NoError(t, err)

	dirent := &snapshot.DirEntry{}
	buffy := bytes.NewBuffer(b)
	decoder := json.NewDecoder(buffy)

	require.NoError(t, decoder.Decode(dirent))

	return dirent
}

func findRealFileDir(t *testing.T, original string) (dir, file string) {
	t.Helper()

	err := filepath.Walk(original, func(path string, info os.FileInfo, err error) error {
		// The file walk shouldn't have generated an error.
		require.NoError(t, err)

		if path == original {
			// The root of the comparison tree is not interesting.
			return nil
		}

		switch {
		case file == "" && info.Mode().IsRegular():
			file = path
		case dir == "" && info.Mode().IsDir():
			dir = path
		case file != "" && dir != "":
			return filepath.SkipDir
		}
		return nil
	})
	require.NoError(t, err)

	return dir, file
}

type repoDirEntryCache struct {
	e           *testenv.CLITest
	direntries  map[string]*snapshot.DirEntry
	rootid      string
	reporootdir string // The absolute directory corresponding to rootid in repo.
}

// repoRootRel returns a directory relative to the path corresponding to
// the rdc.rootid. In particular, the repo entry
// rdc.rootid/repoRootRel(path) should be the same as the localfs whose
// snapshot is rdc.rootid.
func (rdc *repoDirEntryCache) repoRootRel(t *testing.T, fpath string) string {
	t.Helper()

	rp, err := filepath.Rel(rdc.reporootdir, fpath)
	require.NoError(t, err)

	return rp
}

// getRepoDirEntry retrieves the directory entry for rdc.rootid/rop via kopia
// show of the repository rdc.rootid's directory containing rdc.rootid/rop.
// Assumption: repository paths are paths and not filepaths.
func (rdc *repoDirEntryCache) getRepoDirEntry(t *testing.T, rop string) *snapshot.DirEntry {
	t.Helper()

	t.Logf("getRepoDirEntry rop %q", rop)

	if rdc.direntries == nil {
		rdc.direntries = make(map[string]*snapshot.DirEntry)
	}

	rop = filepath.ToSlash(rop)
	repopath := path.Join(rdc.rootid, rop)
	t.Logf("getRepoDirEntry repopath %q", repopath)

	if de, ok := rdc.direntries[repopath]; ok {
		return de
	}

	// Cache miss so fill it up.
	dir := filepath.Dir(rop)
	repodirpath := path.Join(rdc.rootid, dir)
	t.Logf("original directory dir %q containing rop %q giving repodirpath %q", dir, rop, repodirpath)
	spew := rdc.e.RunAndExpectSuccess(t, append([]string{"show"}, repodirpath)...)

	joinedspew := strings.Join(spew, "")

	dirmnst := &snapshot.DirManifest{}
	dirmnstdecoder := json.NewDecoder(strings.NewReader(joinedspew))
	require.NoError(t, dirmnstdecoder.Decode(dirmnst))

	for _, de := range dirmnst.Entries {
		t.Logf("%v", de)
		rdc.direntries[path.Join(repodirpath, de.Name)] = de
	}

	if de, ok := rdc.direntries[repopath]; ok {
		return de
	}

	t.Fatalf("no path %q in repository %v", rop, rdc.rootid)

	return nil
}

// validatePlaceholder checks that shallowrestore absolute path srp has placeholder
// DirEntry value equal to the in-repository DirEntry for rootid/rop.
func (rdc *repoDirEntryCache) validatePlaceholder(t *testing.T, rop, srp string) {
	t.Helper()

	t.Logf("validateXattr comparing rop %q to srp %q", rop, srp)

	dirent := getShallowDirEntry(t, srp)
	de := rdc.getRepoDirEntry(t, rop)

	// I should be able to use reflect instead of the element-by-element comparison.
	if got, want := dirent, de; !reflect.DeepEqual(got, want) {
		t.Errorf("path %q, got from xattr %s, want %s", srp, litter.Sdump(got), litter.Sdump(want))
	}
}

// mutatorArgs holds state useful to filesystemmutator functions.
type mutatorArgs struct {
	t                  *testing.T
	original           string
	shallow            string
	e                  *testenv.CLITest
	originalsnapshotid string
	rdc                *repoDirEntryCache
}

// filesystemmutator functions mutate the provided (via m) original and
// shallow trees with the expectation that snapshots of both trees should
// be same. One function mutates both trees.
type filesystemmutator func(m *mutatorArgs)

// getSnapID gets a snapshot hash for path.
func getSnapID(t *testing.T, sources []clitestutil.SourceInfo, fpath string) string {
	t.Helper()

	for _, si := range sources {
		if fpath == si.Path {
			return si.Snapshots[0].SnapshotID
		}
	}

	t.Fatalf("no snapshot for %q in sources %v", fpath, sources)

	return ""
}

// findFileDir finds directory and file entry paths in a specified shallow
// tree N.B. there will not be any actual directories in the shallow
// tree. Instead, we need to find a file whose metadata says that it
// corresponds to a directory.
func findFileDir(t *testing.T, shallow string) (dirinshallow, fileinshallow string) {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(shallow, "*"))
	require.NoError(t, err)

	for _, f := range files {
		fi, err := os.Lstat(f)
		require.NoError(t, err)

		if !(fi.Mode().IsDir() || fi.Mode().IsRegular()) {
			continue
		}

		// Really long directories can't participate in shallow restores and will
		// be real in a shallow tree. Skip them.
		if !restore.SafelySuffixablePath(f) {
			continue
		}

		switch direntry := getShallowDirEntry(t, f); {
		case direntry.Type == snapshot.EntryTypeFile && fileinshallow == "":
			fileinshallow = f
		case direntry.Type == snapshot.EntryTypeDirectory && dirinshallow == "":
			dirinshallow = f
		}
	}

	return dirinshallow, fileinshallow
}

func getShallowInfo(t *testing.T, srp string) (string, os.FileInfo) {
	t.Helper()

	const ENTRYTYPES = 3
	shallowinfos := make([]os.FileInfo, ENTRYTYPES)
	errors := make([]error, ENTRYTYPES)
	paths := make([]string, ENTRYTYPES)

	v := -1

	for i, s := range []string{"", localfs.ShallowEntrySuffix, dIRPH} {
		paths[i] = srp + s
		shallowinfos[i], errors[i] = os.Lstat(paths[i])

		if errors[i] == nil {
			v = i
		}
	}

	// Always there should be ENTRYTYPES-1 errors (i.e. one and only one of
	// the file paths should exist.)
	errcount := 0

	for _, e := range errors {
		if e != nil {
			errcount++
		}
	}

	switch {
	case errcount == ENTRYTYPES-1:
		return paths[v], shallowinfos[v]
	case errcount < ENTRYTYPES-1:
		nonfaultingpaths := make([]string, 0)

		for i, s := range paths {
			if errors[i] == nil {
				nonfaultingpaths = append(nonfaultingpaths, s)
			}
		}

		t.Errorf("expected only one shallow for %q to exist: %v", srp, strings.Join(nonfaultingpaths, ", "))

		return "", nil
	default:
		t.Errorf("expected a shallow to exist for %q", srp)
		return "", nil
	}
}

// compareShallowToOriginalDir validates that a shallow directory tree
// matches depth levels of its full original where both original and
// shallow are absolute paths and original must be a child directory of
// rdc.reporootdir. depth is w.r.t. original and shallow.
func compareShallowToOriginalDir(t *testing.T, rdc *repoDirEntryCache, original, shallow string, depth int) {
	t.Helper()

	t.Logf("comparing %q and %q", original, shallow)
	err2 := filepath.Walk(original, func(path string, info os.FileInfo, err error) error {
		// The file walk shouldn't have generated an error.
		require.NoError(t, err)

		if path == original {
			// The root of the comparison tree is not interesting.
			return nil
		}

		relpath := rdc.repoRootRel(t, path)
		t.Logf("rp after relativizing relpath %q", relpath)

		rpd := len(strings.Split(relpath, string(filepath.Separator)))
		if rpd > depth {
			srp := filepath.Join(shallow, relpath)
			if _, serr := os.Lstat(srp); serr == nil {
				t.Errorf("shallowrestore insufficiently shallow -- should not have created file %q", srp)
			}
			// This isn't an error. We just skip the rest of the original tree below depth
			// because it shouldn't be in the shallow tree.
			return err
		}

		verifyShallowVsOriginalFile(t, rdc, shallow, relpath, path, depth, info)

		return nil
	})
	require.NoError(t, err2)
}

// verifyShallowVsOriginalFile checks that the path shallow from the
// shallow tree is correct given a corresponding originalpath with
// FileInfo info. There are three cases: relpathdepth > depth: the part
// of the original tree not shallow restored; relpathdepth == depth: the
// shallow placeholders; relpathdepth < depth: the fully restored portion.
//
//nolint:gocyclo,cyclop
func verifyShallowVsOriginalFile(t *testing.T, rdc *repoDirEntryCache, shallow, relpath, opath string, depth int, info os.FileInfo) {
	t.Helper()

	srp := filepath.Join(shallow, relpath)
	placeholderpath, shallowinfo := getShallowInfo(t, srp)
	relpathdepth := len(strings.Split(relpath, string(filepath.Separator)))

	switch {
	case relpathdepth == depth && info.Mode().IsRegular():
		if got, want := shallowinfo.Mode(), info.Mode()&^0o222; got != want {
			t.Errorf(" shallow path %q mode mismatched %q: got %v want %v", srp, opath, got, want)
		}

		if !shallowinfo.Mode().IsRegular() {
			t.Errorf("shallow placeholder %q for original %q not regular", placeholderpath, opath)
		}

		rdc.validatePlaceholder(t, relpath, srp)
	case relpathdepth == depth && info.IsDir():
		if !shallowinfo.Mode().IsRegular() {
			t.Errorf("shallow placeholder %q for original %q not regular", placeholderpath, opath)
		}

		if got, want := shallowinfo.Mode(), info.Mode()&^0o222; got != want {
			t.Errorf(" shallow path %q mode mismatched %q: got %v want %v", placeholderpath, opath, got, want)
		}

		rdc.validatePlaceholder(t, relpath, srp)
	case relpathdepth < depth && info.Mode().IsRegular():
		if got, want := shallowinfo.Size(), info.Size(); got != want {
			t.Errorf(" shallow path %q size mismatched %q: got %v want %v", placeholderpath, opath, got, want)
		}

		if got, want := shallowinfo.Mode(), info.Mode(); got != want {
			t.Errorf(" shallow path %q mode mismatched %q: got %v want %v", placeholderpath, opath, got, want)
		}
	case relpathdepth < depth && info.IsDir():
		if !shallowinfo.IsDir() {
			t.Errorf("non-placeholder entry in shallow tree %q for original %q not dir", placeholderpath, opath)
		}

		if got, want := shallowinfo.Mode(), info.Mode(); got != want {
			t.Errorf(" shallow path %q mode mismatched %q: got %v want %v", placeholderpath, opath, got, want)
		}
	case info.Mode()&os.ModeSymlink > 0:
		// Symlinks are always restored if <= depth.
		if got, want := shallowinfo.Mode(), info.Mode(); got != want {
			t.Errorf("shallow symlink path %q mismatched %q: wrong mode got %v want %v", srp, opath, got, want)
		}
	default:
		t.Errorf("shallow path %q has unanticipated mode %v, original: %q, %v", placeholderpath, shallowinfo.Mode(), opath, info.Mode())
	}

	if shallowinfo.Mode()&os.ModeSymlink > 0 {
		// symlinkChtimes is at best µs precise on Linux
		gt, wt := shallowinfo.ModTime(), info.ModTime()
		if diff := gt.Sub(wt); diff > time.Microsecond {
			t.Errorf("symlink time for %q differs by more than 1 µs: %v", opath, diff)
		}
	} else if got, want := shallowinfo.ModTime(), info.ModTime(); got != want {
		gotstring, _ := got.MarshalJSON()
		wantstring, _ := want.MarshalJSON()

		t.Errorf("path %q shallowrestored wrong time got %v want %v, diff %q", opath, string(gotstring), string(wantstring), got.Sub(want))
	}
}

func makeLongName(c rune) string {
	// TODO(rjk): not likely to work on plan9.
	buffy := make([]byte, 0, restore.MaxFilenameLength)
	for range restore.MaxFilenameLength {
		buffy = append(buffy, byte(c))
	}

	return string(buffy)
}

func mustParseID(t *testing.T, s string) object.ID {
	t.Helper()

	id, err := object.ParseID(s)
	require.NoError(t, err)

	return id
}
