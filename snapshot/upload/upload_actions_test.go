package upload

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestActionHelperProcess(t *testing.T) {
	if os.Getenv("KOPIA_TEST_ACTION_HELPER") != "1" {
		return
	}

	var args []string

	for i, a := range os.Args {
		if a == "--" {
			args = os.Args[i+1:]
			break
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		os.Exit(2)
	}

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--record="):
			recordPath := strings.TrimPrefix(arg, "--record=")
			line := fmt.Sprintf(
				"%s|%s|%s|%s|%s\n",
				os.Getenv("KOPIA_ACTION"),
				os.Getenv("KOPIA_SNAPSHOT_ID"),
				os.Getenv("KOPIA_SOURCE_PATH"),
				os.Getenv("KOPIA_SNAPSHOT_PATH"),
				wd,
			)

			f, ferr := os.OpenFile(recordPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if ferr != nil {
				os.Exit(2)
			}

			_, werr := f.WriteString(line)
			_ = f.Close()

			if werr != nil {
				os.Exit(2)
			}

		case strings.HasPrefix(arg, "--redirect="):
			fmt.Printf("KOPIA_SNAPSHOT_PATH=%s\n", strings.TrimPrefix(arg, "--redirect="))

		case arg == "--fail":
			os.Exit(3)
		}
	}

	os.Exit(0)
}

func helperActionCommand(extraArgs ...string) *policy.ActionCommand {
	args := append([]string{"-test.run=^TestActionHelperProcess$", "--"}, extraArgs...)

	return &policy.ActionCommand{
		Command:   os.Args[0],
		Arguments: args,
		Mode:      "essential",
	}
}

func TestPrepareRootActionsWorkDirCleanup(t *testing.T) {
	t.Setenv("KOPIA_TEST_ACTION_HELPER", "1")

	ctx := testlogging.Context(t)
	srcDir := testutil.TempDirectory(t)
	recordFile := filepath.Join(testutil.TempDirectory(t), "actions.log")

	t.Run("cleanup-on-failure", func(t *testing.T) {
		prepared, err := PrepareRootActions(ctx, policy.ActionsPolicy{
			BeforeSnapshotRoot: helperActionCommand("--record="+recordFile, "--fail"),
		}, srcDir, true)
		require.Error(t, err)
		require.Nil(t, prepared)

		data, rerr := os.ReadFile(recordFile)
		require.NoError(t, rerr)

		parts := strings.Split(strings.TrimSpace(string(data)), "|")
		require.Len(t, parts, 5)
		require.NoDirExists(t, parts[4], "temporary action working directory must be cleaned up when before-snapshot-root fails")
	})

	t.Run("cleanup-after-upload", func(t *testing.T) {
		th := newUploadTestHarness(ctx, t)
		t.Cleanup(th.cleanup)

		u := NewUploader(th.repo)
		u.EnableActions = true

		require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("hello"), 0o600))

		uploadRecord := filepath.Join(testutil.TempDirectory(t), "upload-actions.log")

		pol := *policy.DefaultPolicy
		pol.Actions = policy.ActionsPolicy{
			BeforeSnapshotRoot: helperActionCommand("--record=" + uploadRecord),
			AfterSnapshotRoot:  helperActionCommand("--record=" + uploadRecord),
		}

		policyTree := policy.BuildTree(nil, &pol)

		entry, err := localfs.Directory(srcDir)
		require.NoError(t, err)

		_, err = u.Upload(ctx, entry, policyTree, snapshot.SourceInfo{Path: srcDir})
		require.NoError(t, err)

		data, rerr := os.ReadFile(uploadRecord)
		require.NoError(t, rerr)

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		require.Len(t, lines, 2)

		beforeParts := strings.Split(lines[0], "|")
		afterParts := strings.Split(lines[1], "|")

		require.Equal(t, "before-snapshot-root", beforeParts[0])
		require.Equal(t, "after-snapshot-root", afterParts[0])
		require.Equal(t, beforeParts[1], afterParts[1])
		require.Equal(t, beforeParts[4], afterParts[4])
		require.NoDirExists(t, beforeParts[4], "temporary action working directory must be cleaned up after root actions complete")
	})
}

func TestUploadWithPreparedRootActions(t *testing.T) {
	t.Setenv("KOPIA_TEST_ACTION_HELPER", "1")

	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)
	t.Cleanup(th.cleanup)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "sub"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "sub", "orig.txt"), []byte("orig"), 0o600))

	redirectDir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(redirectDir, "sub"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(redirectDir, "sub", "redirected.txt"), []byte("redirected-content"), 0o600))

	recordFile := filepath.Join(testutil.TempDirectory(t), "prepared.log")

	actions := policy.ActionsPolicy{
		BeforeSnapshotRoot: helperActionCommand("--record="+recordFile, "--redirect="+redirectDir),
		AfterSnapshotRoot:  helperActionCommand("--record=" + recordFile),
	}

	prepared, err := PrepareRootActions(ctx, actions, srcDir, true)
	require.NoError(t, err)
	require.NotNil(t, prepared.OverrideDir())

	pol := *policy.DefaultPolicy
	pol.Actions = actions

	subPol := &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeFolder: helperActionCommand("--record=" + recordFile),
		},
	}

	policyTree := policy.BuildTree(map[string]*policy.Policy{
		"./sub": subPol,
	}, &pol)

	entry, err := localfs.Directory(srcDir)
	require.NoError(t, err)

	u := NewUploader(th.repo)
	u.EnableActions = true
	u.PreparedRootActions = prepared

	man, err := u.Upload(ctx, entry, policyTree, snapshot.SourceInfo{Path: srcDir})
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&man.Stats.TotalFileCount))
	require.Equal(t, int64(len("redirected-content")), atomic.LoadInt64(&man.Stats.TotalFileSize))

	prepared.Cleanup(ctx)

	data, rerr := os.ReadFile(recordFile)
	require.NoError(t, rerr)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 3, "root actions must execute once alongside child folder action")

	beforeRoot := strings.Split(lines[0], "|")
	beforeSub := strings.Split(lines[1], "|")
	afterRoot := strings.Split(lines[2], "|")

	require.Equal(t, "before-snapshot-root", beforeRoot[0])
	require.Equal(t, srcDir, beforeRoot[2])

	require.Equal(t, "before-folder", beforeSub[0])
	require.Equal(t, filepath.Join(srcDir, "sub"), beforeSub[2], "child folder action must preserve logical source path even when root is redirected")

	require.Equal(t, "after-snapshot-root", afterRoot[0])
	require.Equal(t, beforeRoot[1], afterRoot[1])
	require.Equal(t, redirectDir, afterRoot[3])
	require.NoDirExists(t, beforeRoot[4])
}
