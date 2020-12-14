package endtoend_test

import (
	"bufio"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotHooksBeforeSnapshotRoot(t *testing.T) {
	t.Parallel()

	th := os.Getenv("TESTINGHOOK_EXE")
	if th == "" {
		t.Skip("TESTINGHOOK_EXE verifyNoError be set")
	}

	e := testenv.NewCLITest(t)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	envFile1 := filepath.Join(e.LogsDir, "env1.txt")

	// set a hook before-snapshot-root that fails and which saves the environment to a file.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --exit-code=3 --save-env="+envFile1)

	// this prevents the snapshot from being created
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	envFile2 := filepath.Join(e.LogsDir, "env2.txt")

	// now set a hook before-snapshot-root that succeeds and saves environment to a different file
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --save-env="+envFile2)

	// snapshot now succeeds.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	env1 := mustReadEnvFile(t, envFile1)
	env2 := mustReadEnvFile(t, envFile2)

	// make sure snapshot IDs are different between two attempts
	if id1, id2 := env1["KOPIA_SNAPSHOT_ID"], env2["KOPIA_SNAPSHOT_ID"]; id1 == id2 {
		t.Errorf("KOPIA_SNAPSHOT_ID passed to hook was not different between runs %v", id1)
	}

	// Now set up the hook again, in optional mode,
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --exit-code=3",
		"--hook-command-mode=optional")

	// this will not prevent snapshot creation.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// Now set up the hook again, in async mode and pass --sleep so that the command takes some time.
	// because the hook is async it will not wait for the command.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --exit-code=3 --sleep=30s",
		"--hook-command-mode=async")

	t0 := time.Now()

	// at this point the data is all cached so this will be quick, definitely less than 30s,
	// async hook failure will not prevent snapshot success.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	if dur := time.Since(t0); dur > 30*time.Second {
		t.Errorf("command did not execute asynchronously (took %v)", dur)
	}

	// Now set up essential hook with a timeout of 3s and have the hook sleep for 30s
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --sleep=30s",
		"--hook-command-timeout=3s")

	t0 = time.Now()

	// the hook will be killed after 3s and cause a failure.
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	if dur := time.Since(t0); dur > 30*time.Second {
		t.Errorf("command did not apply timeout (took %v)", dur)
	}

	// Now set up essential hook that will cause redirection to an alternative folder which does not exist.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH=/no/such/directory\n"))

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	// Now set up essential hook that will cause redirection to an alternative folder which does exist.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH="+sharedTestDataDir2+"\n"))

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// since we redirected to sharedTestDataDir2 the object ID of last snapshot of sharedTestDataDir1
	// will be the same as snapshots of sharedTestDataDir2
	snaps1 := e.ListSnapshotsAndExpectSuccess(t, sharedTestDataDir1)[0].Snapshots
	snaps2 := e.ListSnapshotsAndExpectSuccess(t, sharedTestDataDir2)[0].Snapshots

	if snaps1[0].ObjectID == snaps2[0].ObjectID {
		t.Fatal("failed sanity check - snapshots are the same")
	}

	if got, want := snaps1[len(snaps1)-1].ObjectID, snaps2[0].ObjectID; got != want {
		t.Fatalf("invalid snapshot ID after redirection %v, wanted %v", got, want)
	}

	// not setup the same redirection but in async mode - will be ignored because Kopia does not wait for asynchronous
	// hooks at all or parse their output.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-hook",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH="+sharedTestDataDir2+"\n"),
		"--hook-command-mode=async")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// verify redirection had no effect - last snapshot will be the same as the first one
	snaps1 = e.ListSnapshotsAndExpectSuccess(t, sharedTestDataDir1)[0].Snapshots
	if got, want := snaps1[len(snaps1)-1].ObjectID, snaps1[0].ObjectID; got != want {
		t.Fatalf("invalid snapshot ID after async hook %v, wanted %v", got, want)
	}
}

func TestSnapshotHooksBeforeAfterFolder(t *testing.T) {
	t.Parallel()

	th := os.Getenv("TESTINGHOOK_EXE")
	if th == "" {
		t.Skip("TESTINGHOOK_EXE verifyNoError be set")
	}

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create directory structure
	rootDir := t.TempDir()
	sd1 := filepath.Join(rootDir, "subdir1")
	sd2 := filepath.Join(rootDir, "subdir2")
	sd11 := filepath.Join(rootDir, "subdir1", "subdir1")
	sd12 := filepath.Join(rootDir, "subdir1", "subdir2")

	verifyNoError(t, os.Mkdir(sd1, 0700))
	verifyNoError(t, os.Mkdir(sd2, 0700))
	verifyNoError(t, os.Mkdir(sd11, 0700))
	verifyNoError(t, os.Mkdir(sd12, 0700))

	hookRanDir := t.TempDir()

	hookRanFileBeforeRoot := filepath.Join(hookRanDir, "before-root")
	hookRanFileAfterRoot := filepath.Join(hookRanDir, "before-root")
	hookRanFileBeforeSD1 := filepath.Join(hookRanDir, "before-sd1")
	hookRanFileAfterSD1 := filepath.Join(hookRanDir, "before-sd1")
	hookRanFileBeforeSD11 := filepath.Join(hookRanDir, "before-sd11")
	hookRanFileAfterSD11 := filepath.Join(hookRanDir, "before-sd11")
	hookRanFileBeforeSD2 := filepath.Join(hookRanDir, "before-sd2")
	hookRanFileAfterSD2 := filepath.Join(hookRanDir, "before-sd2")

	// setup hooks that will write a marker file when the hook is executed.
	//
	// We are not setting a policy on 'sd12' to ensure it's not inherited
	// from sd1. If it was inherited, the hook would fail since it refuses to create the
	// file if one already exists.
	e.RunAndExpectSuccess(t, "policy", "set", rootDir,
		"--before-folder-hook", th+" --create-file="+hookRanFileBeforeRoot)
	e.RunAndExpectSuccess(t, "policy", "set", rootDir,
		"--after-folder-hook", th+" --create-file="+hookRanFileAfterRoot)
	e.RunAndExpectSuccess(t, "policy", "set", sd1,
		"--before-folder-hook", th+" --create-file="+hookRanFileBeforeSD1)
	e.RunAndExpectSuccess(t, "policy", "set", sd1,
		"--after-folder-hook", th+" --create-file="+hookRanFileAfterSD1)
	e.RunAndExpectSuccess(t, "policy", "set", sd2,
		"--before-folder-hook", th+" --create-file="+hookRanFileBeforeSD2)
	e.RunAndExpectSuccess(t, "policy", "set", sd2,
		"--after-folder-hook", th+" --create-file="+hookRanFileAfterSD2)
	e.RunAndExpectSuccess(t, "policy", "set", sd11,
		"--before-folder-hook", th+" --create-file="+hookRanFileBeforeSD11)
	e.RunAndExpectSuccess(t, "policy", "set", sd11,
		"--after-folder-hook", th+" --create-file="+hookRanFileAfterSD11)

	e.RunAndExpectSuccess(t, "snapshot", "create", rootDir)

	verifyFileExists(t, hookRanFileBeforeRoot)
	verifyFileExists(t, hookRanFileAfterRoot)
	verifyFileExists(t, hookRanFileBeforeSD1)
	verifyFileExists(t, hookRanFileBeforeSD11)
	verifyFileExists(t, hookRanFileAfterSD11)
	verifyFileExists(t, hookRanFileAfterSD1)
	verifyFileExists(t, hookRanFileBeforeSD2)
	verifyFileExists(t, hookRanFileAfterSD2)

	// the hook will fail to run the next time since all 'hookRan*' files already exist.
	e.RunAndExpectFailure(t, "snapshot", "create", rootDir)
}

func tmpfileWithContents(t *testing.T, contents string) string {
	f, err := ioutil.TempFile("", "kopia-test")
	verifyNoError(t, err)

	f.WriteString(contents)
	f.Close()

	t.Cleanup(func() { os.Remove(f.Name()) })

	return f.Name()
}

func verifyFileExists(t *testing.T, fname string) {
	t.Helper()

	_, err := os.Stat(fname)
	if err != nil {
		t.Fatal(err)
	}
}

func verifyNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func mustReadEnvFile(t *testing.T, fname string) map[string]string {
	f, err := os.Open(fname)

	verifyNoError(t, err)

	defer f.Close()
	s := bufio.NewScanner(f)

	m := map[string]string{}

	for s.Scan() {
		parts := strings.SplitN(s.Text(), "=", 2)
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}

	verifyNoError(t, s.Err())

	return m
}
