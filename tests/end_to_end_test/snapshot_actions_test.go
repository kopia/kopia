package endtoend_test

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotActionsBeforeSnapshotRoot(t *testing.T) {
	t.Parallel()

	th := skipUnlessTestAction(t)

	logsDir := testutil.TempLogDirectory(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo", "--enable-actions")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	envFile1 := filepath.Join(logsDir, "env1.txt")

	// set a action before-snapshot-root that fails and which saves the environment to a file.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --exit-code=3 --save-env="+envFile1)

	// this prevents the snapshot from being created
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	envFile2 := filepath.Join(logsDir, "env2.txt")

	// now set a action before-snapshot-root that succeeds and saves environment to a different file
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --save-env="+envFile2)

	envFile3 := filepath.Join(logsDir, "env2.txt")

	// set a action after-snapshot-root that succeeds and saves environment to a different file
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--after-snapshot-root-action",
		th+" --save-env="+envFile3)

	// snapshot now succeeds.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	env1 := mustReadEnvFile(t, envFile1)
	env2 := mustReadEnvFile(t, envFile2)
	env3 := mustReadEnvFile(t, envFile3)

	// make sure snapshot IDs are different between two attempts
	require.NotEqual(t, env1["KOPIA_SNAPSHOT_ID"], env2["KOPIA_SNAPSHOT_ID"], "KOPIA_SNAPSHOT_ID passed to action was not different between runs")

	require.Equal(t, env1["KOPIA_ACTION"], "before-snapshot-root")
	require.Equal(t, env3["KOPIA_ACTION"], "after-snapshot-root")
	require.NotEmpty(t, env1["KOPIA_VERSION"])
	require.NotEmpty(t, env3["KOPIA_VERSION"])

	// Now set up the action again, in optional mode,
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --exit-code=3",
		"--action-command-mode=optional")

	// this will not prevent snapshot creation.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// Now set up the action again, in async mode and pass --sleep so that the command takes some time.
	// because the action is async it will not wait for the command.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --exit-code=3 --sleep=30s",
		"--action-command-mode=async")

	timer := timetrack.StartTimer()

	// at this point the data is all cached so this will be quick, definitely less than 30s,
	// async action failure will not prevent snapshot success.
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	if dur := timer.Elapsed(); dur > 30*time.Second {
		t.Errorf("command did not execute asynchronously (took %v)", dur)
	}

	// Now set up essential action with a timeout of 3s and have the action sleep for 30s
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --sleep=30s",
		"--action-command-timeout=3s")

	timer = timetrack.StartTimer()
	// the action will be killed after 3s and cause a failure.
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	if dur := timer.Elapsed(); dur > 30*time.Second {
		t.Errorf("command did not apply timeout (took %v)", dur)
	}

	// Now set up essential action that will cause redirection to an alternative folder which does not exist.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH=/no/such/directory\n"))

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	// Now set up essential action that will cause redirection to an alternative folder which does exist.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH="+sharedTestDataDir2+"\n"))

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// since we redirected to sharedTestDataDir2 the object ID of last snapshot of sharedTestDataDir1
	// will be the same as snapshots of sharedTestDataDir2
	snaps1 := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sharedTestDataDir1)[0].Snapshots
	snaps2 := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sharedTestDataDir2)[0].Snapshots

	if snaps1[0].ObjectID == snaps2[0].ObjectID {
		t.Fatal("failed sanity check - snapshots are the same")
	}

	if got, want := snaps1[len(snaps1)-1].ObjectID, snaps2[0].ObjectID; got != want {
		t.Fatalf("invalid snapshot ID after redirection %v, wanted %v", got, want)
	}

	// not setup the same redirection but in async mode - will be ignored because Kopia does not wait for asynchronous
	// actions at all or parse their output.
	e.RunAndExpectSuccess(t,
		"policy", "set", sharedTestDataDir1,
		"--before-snapshot-root-action",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH="+sharedTestDataDir2+"\n"),
		"--action-command-mode=async")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// verify redirection had no effect - last snapshot will be the same as the first one
	snaps1 = clitestutil.ListSnapshotsAndExpectSuccess(t, e, sharedTestDataDir1)[0].Snapshots
	if got, want := snaps1[len(snaps1)-1].ObjectID, snaps1[0].ObjectID; got != want {
		t.Fatalf("invalid snapshot ID after async action %v, wanted %v", got, want)
	}
}

func TestSnapshotActionsBeforeAfterFolder(t *testing.T) {
	t.Parallel()

	th := skipUnlessTestAction(t)

	logsDir := testutil.TempLogDirectory(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create directory structure
	rootDir := testutil.TempDirectory(t)
	sd1 := filepath.Join(rootDir, "subdir1")
	sd2 := filepath.Join(rootDir, "subdir2")
	sd11 := filepath.Join(rootDir, "subdir1", "subdir1")
	sd12 := filepath.Join(rootDir, "subdir1", "subdir2")

	verifyNoError(t, os.Mkdir(sd1, 0o700))
	verifyNoError(t, os.Mkdir(sd2, 0o700))
	verifyNoError(t, os.Mkdir(sd11, 0o700))
	verifyNoError(t, os.Mkdir(sd12, 0o700))

	actionRanDir := testutil.TempDirectory(t)

	actionRanFileBeforeRoot := filepath.Join(actionRanDir, "before-root")
	actionRanFileAfterRoot := filepath.Join(actionRanDir, "before-root")
	actionRanFileBeforeSD1 := filepath.Join(actionRanDir, "before-sd1")
	actionRanFileAfterSD1 := filepath.Join(actionRanDir, "before-sd1")
	actionRanFileBeforeSD11 := filepath.Join(actionRanDir, "before-sd11")
	actionRanFileAfterSD11 := filepath.Join(actionRanDir, "before-sd11")
	actionRanFileBeforeSD2 := filepath.Join(actionRanDir, "before-sd2")
	actionRanFileAfterSD2 := filepath.Join(actionRanDir, "before-sd2")

	envFile1 := filepath.Join(logsDir, "env1.txt")
	envFile2 := filepath.Join(logsDir, "env2.txt")

	// setup actions that will write a marker file when the action is executed.
	//
	// We are not setting a policy on 'sd12' to ensure it's not inherited
	// from sd1. If it was inherited, the action would fail since it refuses to create the
	// file if one already exists.
	e.RunAndExpectSuccess(t, "policy", "set", rootDir,
		"--before-folder-action", th+" --create-file="+actionRanFileBeforeRoot)
	e.RunAndExpectSuccess(t, "policy", "set", rootDir,
		"--after-folder-action", th+" --create-file="+actionRanFileAfterRoot)
	e.RunAndExpectSuccess(t, "policy", "set", sd1,
		"--before-folder-action", th+" --create-file="+actionRanFileBeforeSD1)
	e.RunAndExpectSuccess(t, "policy", "set", sd1,
		"--after-folder-action", th+" --create-file="+actionRanFileAfterSD1)
	e.RunAndExpectSuccess(t, "policy", "set", sd2,
		"--before-folder-action", th+" --create-file="+actionRanFileBeforeSD2+" --save-env="+envFile1)
	e.RunAndExpectSuccess(t, "policy", "set", sd2,
		"--after-folder-action", th+" --create-file="+actionRanFileAfterSD2+" --save-env="+envFile2)
	e.RunAndExpectSuccess(t, "policy", "set", sd11,
		"--before-folder-action", th+" --create-file="+actionRanFileBeforeSD11)
	e.RunAndExpectSuccess(t, "policy", "set", sd11,
		"--after-folder-action", th+" --create-file="+actionRanFileAfterSD11)

	e.RunAndExpectSuccess(t, "snapshot", "create", rootDir)

	verifyFileExists(t, actionRanFileBeforeRoot)
	verifyFileExists(t, actionRanFileAfterRoot)
	verifyFileExists(t, actionRanFileBeforeSD1)
	verifyFileExists(t, actionRanFileBeforeSD11)
	verifyFileExists(t, actionRanFileAfterSD11)
	verifyFileExists(t, actionRanFileAfterSD1)
	verifyFileExists(t, actionRanFileBeforeSD2)
	verifyFileExists(t, actionRanFileAfterSD2)

	env1 := mustReadEnvFile(t, envFile1)
	env2 := mustReadEnvFile(t, envFile2)

	require.Equal(t, env1["KOPIA_ACTION"], "before-folder")
	require.Equal(t, env2["KOPIA_ACTION"], "after-folder")
	require.Equal(t, env1["KOPIA_SOURCE_PATH"], sd2)
	require.Equal(t, env2["KOPIA_SOURCE_PATH"], sd2)
	require.NotEmpty(t, env1["KOPIA_VERSION"])
	require.NotEmpty(t, env2["KOPIA_VERSION"])

	// the action will fail to run the next time since all 'actionRan*' files already exist.
	e.RunAndExpectFailure(t, "snapshot", "create", rootDir)
}

func TestSnapshotActionsEmbeddedScript(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var (
		successScript      = tmpfileWithContents(t, "echo Hello world!")
		successScript2     string
		failingScript      string
		goodRedirectScript = tmpfileWithContents(t, "echo KOPIA_SNAPSHOT_PATH="+sharedTestDataDir2)
		badRedirectScript  = tmpfileWithContents(t, "echo KOPIA_SNAPSHOT_PATH=/no/such/directory")
	)

	if runtime.GOOS == "windows" {
		failingScript = tmpfileWithContents(t, "exit /b 1")
		successScript2 = tmpfileWithContents(t, "echo Hello world!")
	} else {
		failingScript = tmpfileWithContents(t, "#!/bin/sh\nexit 1")
		successScript2 = tmpfileWithContents(t, "#!/bin/sh\necho Hello world!")
	}

	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--before-folder-action", successScript, "--persist-action-script")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--before-folder-action", goodRedirectScript, "--persist-action-script")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--before-folder-action", successScript2, "--persist-action-script")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	snaps1 := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sharedTestDataDir1)[0].Snapshots
	if snaps1[0].ObjectID == snaps1[1].ObjectID {
		t.Fatalf("redirection did not happen!")
	}

	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--before-folder-action", badRedirectScript, "--persist-action-script")
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--before-folder-action", failingScript, "--persist-action-script")
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)
}

func TestSnapshotActionsWebHook(t *testing.T) {
	t.Run("HTTP", func(t *testing.T) {
		testSnapshotActionsWebHook(t, false)
	})
	t.Run("HTTPS", func(t *testing.T) {
		testSnapshotActionsWebHook(t, true)
	})
}

//nolint:thelper
func testSnapshotActionsWebHook(t *testing.T, tls bool) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcdir, "subdir1"), 0o755))

	mux := http.NewServeMux()

	beforeFolderCount := new(int32)
	afterFolderCount := new(int32)
	beforeSnapshotRootCount := new(int32)
	afterSnapshotRootCount := new(int32)

	mux.HandleFunc("/myhook-before-folder", func(w http.ResponseWriter, r *http.Request) {
		t.Logf("got webhook call: %v", r.RequestURI)
		for k := range r.Header {
			t.Logf("  Header: %v = %v", k, r.Header.Get(k))
		}

		require.NotEmpty(t, r.Header.Get("X-Kopia-Version"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Id"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Path"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Source-Path"))
		require.Equal(t, "before-folder", r.Header.Get("X-Kopia-Action"))
		require.Equal(t, "GET", r.Method)

		atomic.AddInt32(beforeFolderCount, 1)
	})

	mux.HandleFunc("/myhook-after-folder", func(w http.ResponseWriter, r *http.Request) {
		t.Logf("got webhook call: %v", r.RequestURI)
		for k := range r.Header {
			t.Logf("  Header: %v = %v", k, r.Header.Get(k))
		}

		require.NotEmpty(t, r.Header.Get("X-Kopia-Version"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Id"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Path"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Source-Path"))
		require.Equal(t, "after-folder", r.Header.Get("X-Kopia-Action"))
		require.Equal(t, "POST", r.Method)

		atomic.AddInt32(afterFolderCount, 1)
	})

	mux.HandleFunc("/myhook-before-snapshot-root", func(w http.ResponseWriter, r *http.Request) {
		t.Logf("got webhook call: %v", r.RequestURI)
		for k := range r.Header {
			t.Logf("  Header: %v = %v", k, r.Header.Get(k))
		}

		require.NotEmpty(t, r.Header.Get("X-Kopia-Version"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Id"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Path"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Source-Path"))
		require.Equal(t, "before-snapshot-root", r.Header.Get("X-Kopia-Action"))
		require.Equal(t, "bar", r.Header.Get("Foo"))
		require.Equal(t, "Baz", r.Header.Get("Bar"))
		require.Equal(t, "PATCH", r.Method)

		atomic.AddInt32(beforeSnapshotRootCount, 1)
	})

	mux.HandleFunc("/myhook-after-snapshot-root", func(w http.ResponseWriter, r *http.Request) {
		t.Logf("got webhook call: %v", r.RequestURI)
		for k := range r.Header {
			t.Logf("  Header: %v = %v", k, r.Header.Get(k))
		}

		require.NotEmpty(t, r.Header.Get("X-Kopia-Version"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Id"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Snapshot-Path"))
		require.NotEmpty(t, r.Header.Get("X-Kopia-Source-Path"))
		require.Equal(t, "after-snapshot-root", r.Header.Get("X-Kopia-Action"))
		require.Equal(t, "PUT", r.Method)

		atomic.AddInt32(afterSnapshotRootCount, 1)
	})

	var (
		ts        *httptest.Server
		extraArgs []string
	)

	if tls {
		ts = httptest.NewTLSServer(mux)
		fingerprint := sha256.Sum256(ts.Certificate().Raw)

		extraArgs = append(extraArgs,
			"--webhook-server-certificate-fingerprint", hex.EncodeToString(fingerprint[:]))
	} else {
		ts = httptest.NewServer(mux)
	}

	defer ts.Close()

	e.RunAndExpectSuccess(t, append(append([]string{}, "policy", "set", srcdir, "--before-folder-webhook-url", ts.URL+"/myhook-before-folder", "--webhook-method=GET"), extraArgs...)...)
	e.RunAndExpectSuccess(t, append(append([]string{}, "policy", "set", srcdir, "--after-folder-webhook-url", ts.URL+"/myhook-after-folder"), extraArgs...)...)
	e.RunAndExpectSuccess(t, append(append([]string{}, "policy", "set", filepath.Join(srcdir, "subdir1"), "--before-folder-webhook-url", ts.URL+"/myhook-before-folder", "--webhook-method=GET"), extraArgs...)...)
	e.RunAndExpectSuccess(t, append(append([]string{}, "policy", "set", filepath.Join(srcdir, "subdir1"), "--after-folder-webhook-url", ts.URL+"/myhook-after-folder"), extraArgs...)...)
	e.RunAndExpectSuccess(t, append(append([]string{},
		"policy", "set", srcdir, "--before-snapshot-root-webhook-url",
		ts.URL+"/myhook-before-snapshot-root", "--webhook-header", "Foo=bar",
		"--webhook-header", "Bar=Baz", "--webhook-method=PATCH"),
		extraArgs...)...)
	e.RunAndExpectSuccess(t, append(append([]string{},
		"policy", "set", srcdir, "--after-snapshot-root-webhook-url",
		ts.URL+"/myhook-after-snapshot-root", "--webhook-method=PUT"),
		extraArgs...)...)

	lines := testutil.CompressSpaces(e.RunAndExpectSuccess(t, "policy", "show", srcdir))
	require.Contains(t, lines, " Webhook: GET "+ts.URL+"/myhook-before-folder")
	require.Contains(t, lines, " Webhook: PATCH "+ts.URL+"/myhook-before-snapshot-root")
	require.Contains(t, lines, " Webhook: POST "+ts.URL+"/myhook-after-folder")
	require.Contains(t, lines, " Webhook: PUT "+ts.URL+"/myhook-after-snapshot-root")

	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)

	require.EqualValues(t, 1, *beforeSnapshotRootCount)
	require.EqualValues(t, 1, *afterSnapshotRootCount)
	require.EqualValues(t, 2, *beforeFolderCount)
	require.EqualValues(t, 2, *afterFolderCount)

	ts.Close()

	// snapshot fails due to essential webhook not being able to be sent.
	e.RunAndExpectFailure(t, "snapshot", "create", srcdir)
}

func TestSnapshotActionsNonEssentialWebHook(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	srcdir := testutil.TempDirectory(t)

	// setup dummy optional webhook without setting up server
	e.RunAndExpectSuccess(t,
		"policy", "set", srcdir,
		"--before-folder-webhook-url", "http://localhost:41231/no-such-url",
		"--webhook-mode=optional")

	// async webhooks are unsupported
	e.RunAndExpectFailure(t,
		"policy", "set", srcdir,
		"--before-folder-webhook-url", "http://localhost:41231/no-such-url",
		"--webhook-mode=async")

	// snapshot will succeeded because webhook is non-essential.
	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)
}

func TestSnapshotActionsInvalidWebhookTLS(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcdir, "subdir1"), 0o755))

	mux := http.NewServeMux()

	mux.HandleFunc("/myhook-before-folder", func(w http.ResponseWriter, r *http.Request) {
	})

	ts := httptest.NewTLSServer(mux)

	defer ts.Close()

	e.RunAndExpectSuccess(t, "policy", "set", srcdir, "--before-folder-webhook-url", ts.URL+"/myhook-before-folder")
	e.RunAndExpectFailure(t, "snapshot", "create", srcdir)
}

func TestSnapshotActionsWebHookRedirect(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	srcdir := testutil.TempDirectory(t)
	srcdir2 := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcdir2, "foo"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(srcdir2, "bar"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(srcdir2, "baz"), 0o755))

	mux := http.NewServeMux()

	mux.HandleFunc("/myhook-before", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, r.Header.Get("X-Kopia-Source-Path"), r.Header.Get("X-Kopia-Snapshot-Path"))

		w.Header().Add("X-Kopia-Snapshot-Path", srcdir2)
	})

	mux.HandleFunc("/myhook-after", func(w http.ResponseWriter, r *http.Request) {
		require.NotEqual(t, r.Header.Get("X-Kopia-Source-Path"), r.Header.Get("X-Kopia-Snapshot-Path"))
	})

	ts := httptest.NewServer(mux)

	defer ts.Close()

	e.RunAndExpectSuccess(t, "policy", "set", srcdir, "--before-snapshot-root-webhook-url", ts.URL+"/myhook-before")
	e.RunAndExpectSuccess(t, "policy", "set", srcdir, "--after-snapshot-root-webhook-url", ts.URL+"/myhook-after")

	var man snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", srcdir, "--json"), &man)

	// make sure we snapshotted second directory 1 + 3 subdirectories.
	require.Equal(t, int64(4), man.RootEntry.DirSummary.TotalDirCount)
}

func TestSnapshotActionsWebHookFailure(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--enable-actions")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	srcdir := testutil.TempDirectory(t)

	mux := http.NewServeMux()

	mux.HandleFunc("/myhook-before", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	ts := httptest.NewServer(mux)

	defer ts.Close()

	e.RunAndExpectSuccess(t, "policy", "set", srcdir, "--before-snapshot-root-webhook-url", ts.URL+"/myhook-before")
	e.RunAndExpectFailure(t, "snapshot", "create", srcdir)
}

func TestSnapshotActionsEnable(t *testing.T) {
	t.Parallel()

	th := skipUnlessTestAction(t)

	cases := []struct {
		desc          string
		connectFlags  []string
		snapshotFlags []string
		wantRun       bool
	}{
		{desc: "defaults", connectFlags: nil, snapshotFlags: nil, wantRun: false},
		{desc: "override-connect-disable", connectFlags: []string{"--enable-actions"}, snapshotFlags: nil, wantRun: true},
		{desc: "override-connect-disable", connectFlags: []string{"--no-enable-actions"}, snapshotFlags: nil, wantRun: false},
		{desc: "override-snapshot-enable", connectFlags: nil, snapshotFlags: []string{"--force-enable-actions"}, wantRun: true},
		{desc: "override-snapshot-disable", connectFlags: nil, snapshotFlags: []string{"--force-disable-actions"}, wantRun: false},
		{desc: "snapshot-takes-precedence-enable", connectFlags: []string{"--no-enable-actions"}, snapshotFlags: []string{"--force-enable-actions"}, wantRun: true},
		{desc: "snapshot-takes-precedence-disable", connectFlags: []string{"--enable-actions"}, snapshotFlags: []string{"--force-disable-actions"}, wantRun: false},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			logsDir := testutil.TempLogDirectory(t)

			runner := testenv.NewInProcRunner(t)
			e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

			defer e.RunAndExpectSuccess(t, "repo", "disconnect")

			e.RunAndExpectSuccess(t, append([]string{"repo", "create", "filesystem", "--path", e.RepoDir}, tc.connectFlags...)...)

			envFile := filepath.Join(logsDir, "env1.txt")

			// set an action before-snapshot-root that fails and which saves the environment to a file.
			e.RunAndExpectSuccess(t,
				"policy", "set",
				sharedTestDataDir1,
				"--before-snapshot-root-action",
				th+" --save-env="+envFile)

			e.RunAndExpectSuccess(t, append([]string{"snapshot", "create", sharedTestDataDir1}, tc.snapshotFlags...)...)

			_, err := os.Stat(envFile)
			didRun := err == nil
			if didRun != tc.wantRun {
				t.Errorf("unexpected behavior. did run: %v want run: %v", didRun, tc.wantRun)
			}
		})
	}
}

func tmpfileWithContents(t *testing.T, contents string) string {
	t.Helper()

	f, err := os.CreateTemp("", "kopia-test")
	verifyNoError(t, err)

	f.WriteString(contents)
	f.Close()

	t.Cleanup(func() { os.Remove(f.Name()) })

	return f.Name()
}

func verifyFileExists(t *testing.T, fname string) {
	t.Helper()

	_, err := os.Stat(fname)
	require.NoError(t, err)
}

func verifyNoError(t *testing.T, err error) {
	t.Helper()

	require.NoError(t, err)
}

func mustReadEnvFile(t *testing.T, fname string) map[string]string {
	t.Helper()

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

func TestSnapshotActionsHonorIgnoreRules(t *testing.T) {
	t.Parallel()

	th := skipUnlessTestAction(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo", "--enable-actions")

	sourceDir := testutil.TempDirectory(t)
	redirectedDir := testutil.TempDirectory(t)

	os.WriteFile(filepath.Join(redirectedDir, "some-file"), []byte{1, 2, 3}, 0o666)
	os.WriteFile(filepath.Join(redirectedDir, "some-ignored-file"), []byte{1, 2, 3}, 0o666)
	os.WriteFile(filepath.Join(redirectedDir, ".kopiaignore"), []byte(`
some-ignored-file
`), 0o666)

	// set up action that redirects sourceDir to redirectedDir, simulating a filesystem
	// snapshot situation
	e.RunAndExpectSuccess(t,
		"policy", "set", sourceDir,
		"--before-snapshot-root-action",
		th+" --stdout-file="+tmpfileWithContents(t, "KOPIA_SNAPSHOT_PATH="+redirectedDir+"\n"))

	var man snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sourceDir, "--json"), &man)
	entries := e.RunAndExpectSuccess(t, "ls", man.RootObjectID().String())

	require.Contains(t, entries, ".kopiaignore")
	require.Contains(t, entries, "some-file")

	// make sure .kopiaignore was honored
	require.NotContains(t, entries, "some-ignored-file")
}

func skipUnlessTestAction(t *testing.T) string {
	t.Helper()

	th := os.Getenv("TESTING_ACTION_EXE")
	if th == "" {
		t.Skip("TESTING_ACTION_EXE must be set")
	}

	if _, err := os.Stat(th); os.IsNotExist(err) {
		t.Fatal("TESTING_ACTION_EXE does not exist")
	}

	return th
}
