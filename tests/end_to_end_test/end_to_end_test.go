package endtoend_test

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/internal/fshasher"
)

const repoPassword = "qWQPJ2hiiLgWRRCr" // nolint:gosec

type testenv struct {
	repoDir   string
	configDir string
	dataDir   string
	exe       string

	fixedArgs   []string
	environment []string
}

type sourceInfo struct {
	user      string
	host      string
	path      string
	snapshots []snapshotInfo
}

type snapshotInfo struct {
	objectID   string
	snapshotID string
	time       time.Time
}

func newTestEnv(t *testing.T) *testenv {
	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		t.Skip("KOPIA_EXE not set in the environment, skipping test")
	}

	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	configDir, err := ioutil.TempDir("", "kopia-config")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	dataDir, err := ioutil.TempDir("", "kopia-data")
	if err != nil {
		t.Fatalf("can't create temp directory: %v", err)
	}

	return &testenv{
		repoDir:   repoDir,
		configDir: configDir,
		dataDir:   dataDir,
		exe:       exe,
		fixedArgs: []string{
			// use per-test config file, to avoid clobbering current user's setup.
			"--config-file", filepath.Join(configDir, ".kopia.config"),
		},
		environment: []string{"KOPIA_PASSWORD=" + repoPassword},
	}
}

func (e *testenv) cleanup(t *testing.T) {
	if t.Failed() {
		t.Logf("skipped cleanup for failed test, examine repository: %v", e.repoDir)
		return
	}

	if e.repoDir != "" {
		os.RemoveAll(e.repoDir)
	}

	if e.configDir != "" {
		os.RemoveAll(e.configDir)
	}

	if e.dataDir != "" {
		os.RemoveAll(e.dataDir)
	}
}

//nolint:funlen
func TestEndToEnd(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	// make sure we can read policy
	e.runAndExpectSuccess(t, "policy", "show", "--global")

	t.Run("VerifyGlobalPolicy", func(t *testing.T) {
		// verify we created global policy entry
		globalPolicyBlockID := e.runAndVerifyOutputLineCount(t, 1, "content", "ls")[0]
		e.runAndExpectSuccess(t, "content", "show", "-jz", globalPolicyBlockID)

		// make sure the policy is visible in the manifest list
		e.runAndVerifyOutputLineCount(t, 1, "manifest", "list", "--filter=type:policy", "--filter=policyType:global")

		// make sure the policy is visible in the policy list
		e.runAndVerifyOutputLineCount(t, 1, "policy", "list")
	})

	t.Run("Reconnect", func(t *testing.T) {
		e.runAndExpectSuccess(t, "repo", "disconnect")
		e.runAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.repoDir)
		e.runAndExpectSuccess(t, "repo", "status")
	})

	t.Run("ReconnectUsingToken", func(t *testing.T) {
		lines := e.runAndExpectSuccess(t, "repo", "status", "-t", "-s")
		prefix := "$ kopia "
		var reconnectArgs []string

		// look for output line containing the prefix - this will be our reconnect command
		for _, l := range lines {
			if strings.HasPrefix(l, prefix) {
				reconnectArgs = strings.Split(strings.TrimPrefix(l, prefix), " ")
			}
		}

		if reconnectArgs == nil {
			t.Fatalf("can't find reonnect command in kopia repo status output")
		}

		e.runAndExpectSuccess(t, "repo", "disconnect")
		e.runAndExpectSuccess(t, reconnectArgs...)
		e.runAndExpectSuccess(t, "repo", "status")
	})

	e.runAndExpectSuccess(t, "snapshot", "create", ".")
	e.runAndExpectSuccess(t, "snapshot", "list", ".")

	dir1 := filepath.Join(e.dataDir, "dir1")
	createDirectory(t, dir1, 3)
	e.runAndExpectSuccess(t, "snapshot", "create", dir1)
	e.runAndExpectSuccess(t, "snapshot", "create", dir1)

	dir2 := filepath.Join(e.dataDir, "dir2")
	createDirectory(t, dir2, 3)
	e.runAndExpectSuccess(t, "snapshot", "create", dir2)
	e.runAndExpectSuccess(t, "snapshot", "create", dir2)

	dir3 := filepath.Join(e.dataDir, "dir3")
	createDirectory(t, dir3, 3)
	e.runAndExpectSuccess(t, "snapshot", "create", "--hostname", "bar", "--username", "foo", dir3)
	e.runAndExpectSuccess(t, "snapshot", "list", "--hostname", "bar", "--username", "foo", dir3)

	sources := listSnapshotsAndExpectSuccess(t, e)
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}

	// expect 7 blobs, each snapshot creation adds one index blob
	e.runAndVerifyOutputLineCount(t, 7, "index", "ls")
	e.runAndExpectSuccess(t, "index", "optimize")
	e.runAndVerifyOutputLineCount(t, 1, "index", "ls")

	e.runAndExpectSuccess(t, "snapshot", "create", ".", dir1, dir2)
	e.runAndVerifyOutputLineCount(t, 2, "index", "ls")

	t.Run("Migrate", func(t *testing.T) {
		dstenv := newTestEnv(t)
		defer dstenv.cleanup(t)
		defer dstenv.runAndExpectSuccess(t, "repo", "disconnect")

		dstenv.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", dstenv.repoDir)
		dstenv.runAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.configDir, ".kopia.config"), "--all")
		// migrate again, which should be a no-op.
		dstenv.runAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.configDir, ".kopia.config"), "--all")

		sourceSnapshotCount := len(e.runAndExpectSuccess(t, "snapshot", "list", ".", "-a"))
		dstenv.runAndVerifyOutputLineCount(t, sourceSnapshotCount, "snapshot", "list", ".", "-a")
	})

	t.Run("RepairIndexBlobs", func(t *testing.T) {
		contentsBefore := e.runAndExpectSuccess(t, "content", "ls")

		lines := e.runAndVerifyOutputLineCount(t, 2, "index", "ls")
		for _, l := range lines {
			indexFile := strings.Split(l, " ")[0]
			e.runAndExpectSuccess(t, "blob", "delete", indexFile)
		}

		// there should be no index files at this point
		e.runAndVerifyOutputLineCount(t, 0, "index", "ls", "--no-list-caching")
		// there should be no blocks, since there are no indexesto find them
		e.runAndVerifyOutputLineCount(t, 0, "content", "ls")

		// now recover index from all blocks
		e.runAndExpectSuccess(t, "index", "recover", "--commit")

		// all recovered index entries are added as index file
		e.runAndVerifyOutputLineCount(t, 1, "index", "ls")
		contentsAfter := e.runAndExpectSuccess(t, "content", "ls")
		if d := pretty.Compare(contentsBefore, contentsAfter); d != "" {
			t.Errorf("unexpected block diff after recovery: %v", d)
		}
	})

	t.Run("RepairFormatBlob", func(t *testing.T) {
		// remove kopia.repository
		e.runAndExpectSuccess(t, "blob", "rm", "kopia.repository")
		e.runAndExpectSuccess(t, "repo", "disconnect")

		// this will fail because the format blob in the repository is not found
		e.runAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e.repoDir)

		// now run repair, which will recover the format blob from one of the pack blobs.
		e.runAndExpectSuccess(t, "repo", "repair", "--log-level=debug", "--trace-storage", "filesystem", "--path", e.repoDir)

		// now connect can succeed
		e.runAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.repoDir)
	})
}

func TestSnapshotGC(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	expectedContentCount := len(e.runAndExpectSuccess(t, "content", "list"))

	dataDir := filepath.Join(e.dataDir, "dir1")
	assertNoError(t, os.MkdirAll(dataDir, 0777))
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0600))

	// take a snapshot of a directory with 1 file
	e.runAndExpectSuccess(t, "snap", "create", dataDir)

	// data block + directory block + manifest block
	expectedContentCount += 3
	e.runAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// now delete all manifests, making the content unreachable
	for _, line := range e.runAndExpectSuccess(t, "snap", "list", "-m") {
		p := strings.Index(line, "manifest:")
		if p >= 0 {
			manifestID := strings.TrimPrefix(strings.Split(line[p:], " ")[0], "manifest:")
			t.Logf("manifestID: %v", manifestID)
			e.runAndExpectSuccess(t, "manifest", "rm", manifestID)
		}
	}

	// deletion of manifests creates a new manifest
	expectedContentCount++

	// run verification
	e.runAndExpectSuccess(t, "snapshot", "verify", "--all-sources")

	// garbage-collect in dry run mode
	e.runAndExpectSuccess(t, "snapshot", "gc")

	// data block + directory block + manifest block + manifest block from manifest deletion
	e.runAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// garbage-collect for real, but contents are too recent so won't be deleted
	e.runAndExpectSuccess(t, "snapshot", "gc", "--delete")

	// data block + directory block + manifest block + manifest block from manifest deletion
	e.runAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// garbage-collect for real, this time without age limit
	e.runAndExpectSuccess(t, "snapshot", "gc", "--delete", "--min-age", "0s")

	// two contents are deleted
	expectedContentCount -= 2
	e.runAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")
}

type deleteArgMaker func(manifestID string, source sourceInfo) []string

func TestSnapshotDelete(t *testing.T) {
	expectFail := false
	expectSuccess := true
	for _, tc := range []struct {
		desc          string
		mf            deleteArgMaker
		expectSuccess bool
	}{
		{
			"Test manifest rm function",
			func(manifestID string, source sourceInfo) []string {
				return []string{"manifest", "rm", manifestID}
			},
			expectSuccess,
		},
		{
			"Specify all source values correctly",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", source.user,
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"Specify path and username, using default hostname",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--username", source.user,
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"Specify path and hostname, using default username",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"No source flags, with unsafe ignore source flag",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--unsafe-ignore-source",
				}
			},
			expectSuccess,
		},
		{
			"Specify path only, using default username and hostname",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"Specify all source flags, incorrect host name",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", "some-other-host",
					"--username", source.user,
					"--path", source.path,
				}
			},
			expectFail,
		},
		{
			"Specify all source flags, incorrect user name",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", "some-other-user",
					"--path", source.path,
				}
			},
			expectFail,
		},
		{
			"Specify all source flags, incorrect path",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", source.user,
					"--path", "some-wrong-path",
				}
			},
			expectFail,
		},
		{
			"Specify all source flags, incorrect hostname, ignore flag set",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--unsafe-ignore-source",
					"--hostname", "some-other-host",
					"--username", source.user,
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"Specify all source flags, incorrect username, ignore flag set",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", "some-other-user",
					"--unsafe-ignore-source",
					"--path", source.path,
				}
			},
			expectSuccess,
		},
		{
			"Specify all source flags, incorrect path, ignore flag set",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", source.user,
					"--path", "some-wrong-path",
					"--unsafe-ignore-source",
				}
			},
			expectSuccess,
		},
		{
			"No manifest ID provided",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete"}
			},
			expectFail,
		},
		{
			"No manifest ID provided, ignore source flag set",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete",
					"--unsafe-ignore-source",
				}
			},
			expectFail,
		},
		{
			"Garbage manifest ID provided",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", "some-garbage-manifestID"}
			},
			expectFail,
		},
		{
			"Hostname flag provided but no value input",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname",
					"--username", source.user,
					"--path", source.path,
				}
			},
			expectFail,
		},
		{
			"No path provided and no unsafe ignore source flag provided",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID}
			},
			expectFail,
		},
		{
			"Specify hostname and username with no path provided",
			func(manifestID string, source sourceInfo) []string {
				return []string{"snapshot", "delete", manifestID,
					"--hostname", source.host,
					"--username", source.user,
				}
			},
			expectFail,
		},
	} {
		t.Log(tc.desc)
		testSnapshotDelete(t, tc.mf, tc.expectSuccess)
	}
}

func testSnapshotDelete(t *testing.T, argMaker deleteArgMaker, expectDeleteSucceeds bool) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	dataDir := filepath.Join(e.dataDir, "dir1")
	assertNoError(t, os.MkdirAll(dataDir, 0777))
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0600))

	// take a snapshot of a directory with 1 file
	e.runAndExpectSuccess(t, "snap", "create", dataDir)

	// now delete all manifests, making the content unreachable
	si := listSnapshotsAndExpectSuccess(t, e, dataDir)
	for _, source := range si {
		for _, ss := range source.snapshots {
			manifestID := ss.snapshotID
			t.Logf("manifestID: %v", manifestID)
			args := argMaker(manifestID, source)
			if expectDeleteSucceeds {
				e.runAndExpectSuccess(t, args...)
			} else {
				e.runAndExpectFailure(t, args...)
			}
		}
	}
}

func TestSnapshotDeleteTypeCheck(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	lines := e.runAndExpectSuccess(t, "manifest", "ls")
	if len(lines) != 1 {
		t.Fatalf("Expected 1 line global policy output for manifest ls")
	}
	line := lines[0]
	fields := strings.Fields(line)
	manifestID := fields[0]
	typeField := fields[5]
	typeVal := strings.TrimPrefix(typeField, "type:")
	if typeVal != "policy" {
		t.Fatalf("Expected global policy manifest on a fresh repo")
	}

	e.runAndExpectFailure(t, "snapshot", "delete", manifestID, "--unsafe-ignore-source")
}

func TestSnapshotDeleteRestore(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	source := filepath.Join(e.dataDir, "source")
	createDirectory(t, source, 1)
	restoreDir := filepath.Join(e.dataDir, "restored")

	// Create snapshot
	e.runAndExpectSuccess(t, "snapshot", "create", source)

	// obtain snapshot root id and use it for restore
	si := listSnapshotsAndExpectSuccess(t, e, source)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}
	if got, want := len(si[0].snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}
	snapID := si[0].snapshots[0].snapshotID
	rootID := si[0].snapshots[0].objectID

	e.runAndExpectSuccess(t, "restore", rootID, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	assertNoError(t, os.Chmod(restoreDir, 0700))
	compareDirs(t, source, restoreDir)

	// snapshot delete should succeed
	e.runAndExpectSuccess(t, "snapshot", "delete", snapID,
		"--unsafe-ignore-source",
	)

	// Subsequent snapshot delete to the same ID should fail
	e.runAndExpectFailure(t, "snapshot", "delete", snapID,
		"--unsafe-ignore-source",
	)

	// garbage-collect to clean up the root object. Otherwise
	// a restore will succeed
	e.runAndExpectSuccess(t, "snapshot", "gc", "--delete", "--min-age", "0s")

	// Run a restore on the deleted snapshot's root ID
	notRestoreDir := filepath.Join(e.dataDir, "notrestored")
	e.runAndExpectFailure(t, "restore", rootID, notRestoreDir)

	// Make sure the restore did not happen from the deleted snapshot
	fileInfo, err := ioutil.ReadDir(notRestoreDir)
	assertNoError(t, err)
	if len(fileInfo) != 0 {
		t.Fatalf("expected nothing to be restored")
	}
}

func TestDiff(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	dataDir := filepath.Join(e.dataDir, "dir1")

	// initial snapshot
	assertNoError(t, os.MkdirAll(dataDir, 0777))
	e.runAndExpectSuccess(t, "snapshot", "create", dataDir)

	// create some directories and files
	assertNoError(t, os.MkdirAll(filepath.Join(dataDir, "foo"), 0700))
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0600))
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
dog
`), 0600))
	e.runAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
canary
`), 0600))

	assertNoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0700))
	e.runAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	os.Remove(filepath.Join(dataDir, "some-file1"))

	assertNoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0700))
	e.runAndExpectSuccess(t, "snapshot", "create", dataDir)

	si := listSnapshotsAndExpectSuccess(t, e, dataDir)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	// make sure we can generate between all versions of the directory
	snapshots := si[0].snapshots
	for _, s1 := range snapshots {
		for _, s2 := range snapshots {
			e.runAndExpectSuccess(t, "diff", "-f", s1.objectID, s2.objectID)
		}
	}
}

func TestSnapshotRestore(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	source := filepath.Join(e.dataDir, "source")
	createDirectory(t, source, 1)

	restoreDir := filepath.Join(e.dataDir, "restored")
	// Attempt to restore a snapshot from an empty repo.
	e.runAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80faa9", "r1")
	e.runAndExpectSuccess(t, "snapshot", "create", source)
	o := e.runAndExpectSuccess(t, "snapshot", "list")

	// obtain snapshot root id and use it for restore
	root := getLastSnapshotRootID(t, o)
	t.Log("root id: ", root)

	// Attempt to restore a non-existing snapshot.
	e.runAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80fdd9", "r2")

	// Ensure restored files are created with a different ModTime
	time.Sleep(time.Second)

	// Restore last snapshot
	e.runAndExpectSuccess(t, "restore", root, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	assertNoError(t, os.Chmod(restoreDir, 0700))

	compareDirs(t, source, restoreDir)
}

func compareDirs(t *testing.T, source, restoreDir string) {
	// Restored contents should match source
	s, err := localfs.Directory(source)
	assertNoError(t, err)
	wantHash, err := fshasher.Hash(context.Background(), s)
	assertNoError(t, err)

	// check restored contents
	r, err := localfs.Directory(restoreDir)
	assertNoError(t, err)

	ctx := context.Background()
	gotHash, err := fshasher.Hash(ctx, r)
	assertNoError(t, err)

	if !assert.Equal(t, wantHash, gotHash, "restored directory hash does not match source's hash") {
		cmp, err := diff.NewComparer(os.Stderr)
		assertNoError(t, err)

		cmp.DiffCommand = "cmp"
		_ = cmp.Compare(ctx, s, r)
	}
}

func TestCompression(t *testing.T) {
	e := newTestEnv(t)
	defer e.cleanup(t)
	defer e.runAndExpectSuccess(t, "repo", "disconnect")

	e.runAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.repoDir)

	// set global policy
	e.runAndExpectSuccess(t, "policy", "set", "--global", "--compression", "pgzip")

	dataDir := filepath.Join(e.dataDir, "dir1")
	assertNoError(t, os.MkdirAll(dataDir, 0777))

	dataLines := []string{
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
	}
	// add a file that compresses well
	assertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(strings.Join(dataLines, "\n")), 0600))

	e.runAndExpectSuccess(t, "snapshot", "create", dataDir)
	sources := listSnapshotsAndExpectSuccess(t, e)
	oid := sources[0].snapshots[0].objectID
	entries := listDirectory(t, e, oid)

	if !strings.HasPrefix(entries[0].oid, "Z") {
		t.Errorf("expected compressed object, got %v", entries[0].oid)
	}

	if lines := e.runAndExpectSuccess(t, "show", entries[0].oid); !reflect.DeepEqual(dataLines, lines) {
		t.Errorf("invalid object contents")
	}
}

func (e *testenv) runAndExpectSuccess(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, err := e.run(t, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout
}

func (e *testenv) runAndExpectFailure(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, err := e.run(t, args...)
	if err == nil {
		t.Fatalf("'kopia %v' succeeded, but expected failure", strings.Join(args, " "))
	}

	return stdout
}

func (e *testenv) runAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	t.Helper()

	lines := e.runAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Errorf("unexpected list of results of 'kopia %v': %v (%v lines), wanted %v", strings.Join(args, " "), lines, len(lines), wantLines)
	}

	return lines
}

func (e *testenv) run(t *testing.T, args ...string) ([]string, error) {
	t.Helper()
	t.Logf("running 'kopia %v'", strings.Join(args, " "))
	// nolint:gosec
	cmdArgs := append(append([]string(nil), e.fixedArgs...), args...)

	// nolint:gosec
	c := exec.Command(e.exe, cmdArgs...)
	c.Env = append(os.Environ(), e.environment...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		t.Fatalf("can't set up stderr pipe reader")
	}

	var stderr []byte

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		stderr, err = ioutil.ReadAll(stderrPipe)
	}()

	o, err := c.Output()

	wg.Wait()
	t.Logf("finished 'kopia %v' with err=%v and output:\n%v\nstderr:\n%v\n", strings.Join(args, " "), err, trimOutput(string(o)), trimOutput(string(stderr)))

	return splitLines(string(o)), err
}

func trimOutput(s string) string {
	lines := splitLines(s)
	if len(lines) <= 100 {
		return s
	}

	lines2 := append([]string(nil), lines[0:50]...)
	lines2 = append(lines2, fmt.Sprintf("/* %v lines removed */", len(lines)-100))
	lines2 = append(lines2, lines[len(lines)-50:]...)

	return strings.Join(lines2, "\n")
}

func listSnapshotsAndExpectSuccess(t *testing.T, e *testenv, targets ...string) []sourceInfo {
	lines := e.runAndExpectSuccess(t, append([]string{"snapshot", "list", "-l", "--manifest-id"}, targets...)...)
	return mustParseSnapshots(t, lines)
}

type dirEntry struct {
	name string
	oid  string
}

func listDirectory(t *testing.T, e *testenv, targets ...string) []dirEntry {
	lines := e.runAndExpectSuccess(t, append([]string{"ls", "-l"}, targets...)...)
	return mustParseDirectoryEntries(lines)
}

func mustParseDirectoryEntries(lines []string) []dirEntry {
	var result []dirEntry

	for _, l := range lines {
		parts := strings.Fields(l)

		result = append(result, dirEntry{
			name: parts[6],
			oid:  parts[5],
		})
	}

	return result
}

func createDirectory(t *testing.T, dirname string, depth int) {
	if err := os.MkdirAll(dirname, 0700); err != nil {
		t.Fatalf("unable to create directory %v: %v", dirname, err)
	}

	if depth > 0 {
		numSubDirs := rand.Intn(10) + 1
		for i := 0; i < numSubDirs; i++ {
			subdirName := randomName()

			createDirectory(t, filepath.Join(dirname, subdirName), depth-1)
		}
	}

	numFiles := rand.Intn(10) + 1
	for i := 0; i < numFiles; i++ {
		fileName := randomName()

		createRandomFile(t, filepath.Join(dirname, fileName))
	}
}

func createRandomFile(t *testing.T, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		t.Fatalf("unable to create random file: %v", err)
	}
	defer f.Close()

	length := rand.Int63n(100000)
	_, err = io.Copy(f, io.LimitReader(rand.New(rand.NewSource(1)), length))
	assertNoError(t, err)
}

func mustParseSnapshots(t *testing.T, lines []string) []sourceInfo {
	var result []sourceInfo

	var currentSource *sourceInfo

	for _, l := range lines {
		if l == "" {
			continue
		}

		if strings.HasPrefix(l, "  ") {
			if currentSource == nil {
				t.Errorf("snapshot without a source: %q", l)
				return nil
			}

			currentSource.snapshots = append(currentSource.snapshots, mustParseSnaphotInfo(t, l[2:]))

			continue
		}

		s := mustParseSourceInfo(t, l)
		result = append(result, s)
		currentSource = &result[len(result)-1]
	}

	return result
}

func randomName() string {
	b := make([]byte, rand.Intn(10)+3)
	cryptorand.Read(b) // nolint:errcheck

	return hex.EncodeToString(b)
}

func mustParseSnaphotInfo(t *testing.T, l string) snapshotInfo {
	parts := strings.Split(l, " ")

	ts, err := time.Parse("2006-01-02 15:04:05 MST", strings.Join(parts[0:3], " "))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	manifestField := parts[7]
	snapID := strings.TrimPrefix(manifestField, "manifest:")

	return snapshotInfo{
		time:       ts,
		objectID:   parts[3],
		snapshotID: snapID,
	}
}

func mustParseSourceInfo(t *testing.T, l string) sourceInfo {
	p1 := strings.Index(l, "@")

	p2 := strings.Index(l, ":")

	if p1 >= 0 && p2 > p1 {
		return sourceInfo{user: l[0:p1], host: l[p1+1 : p2], path: l[p2+1:]}
	}

	t.Fatalf("can't parse source info: %q", l)

	return sourceInfo{}
}

func splitLines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var result []string
	for _, l := range strings.Split(s, "\n") {
		result = append(result, strings.TrimRight(l, "\r"))
	}

	return result
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func getLastSnapshotRootID(t *testing.T, listOutput []string) string {
	t.Helper()

	if len(listOutput) == 0 {
		t.Fatal("Expected non-empty snapshot list")
	}

	f := strings.Fields(listOutput[len(listOutput)-1])
	if len(f) < 4 {
		t.Fatal("Could not parse snapshot list output: ", listOutput)
	}

	return f[3]
}
