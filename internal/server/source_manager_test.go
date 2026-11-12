package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestSourceManagerActionHelperProcess(t *testing.T) {
	if os.Getenv("KOPIA_SERVER_TEST_ACTION_HELPER") != "1" {
		return
	}

	var args []string

	for i, a := range os.Args {
		if a == "--" {
			args = os.Args[i+1:]
			break
		}
	}

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--record="):
			recordPath := strings.TrimPrefix(arg, "--record=")
			line := fmt.Sprintf(
				"%s|%s|%s|%s\n",
				os.Getenv("KOPIA_ACTION"),
				os.Getenv("KOPIA_SNAPSHOT_ID"),
				os.Getenv("KOPIA_SOURCE_PATH"),
				os.Getenv("KOPIA_SNAPSHOT_PATH"),
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

		case strings.HasPrefix(arg, "--create-source="):
			dir := strings.TrimPrefix(arg, "--create-source=")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				os.Exit(2)
			}

			if err := os.WriteFile(filepath.Join(dir, "mounted.txt"), []byte("mounted-data"), 0o600); err != nil {
				os.Exit(2)
			}

		case strings.HasPrefix(arg, "--remove-source="):
			if err := os.RemoveAll(strings.TrimPrefix(arg, "--remove-source=")); err != nil {
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

func serverTestActionCommand(mode string, extraArgs ...string) *policy.ActionCommand {
	args := append([]string{"-test.run=^TestSourceManagerActionHelperProcess$", "--"}, extraArgs...)

	return &policy.ActionCommand{
		Command:   os.Args[0],
		Arguments: args,
		Mode:      mode,
	}
}

type countingRepository struct {
	repo.Repository

	enableActions  bool
	newWriterCalls atomic.Int32
	failNewWriter  atomic.Bool
}

type countingRepositoryWriter struct {
	repo.RepositoryWriter

	enableActions bool
}

func (w *countingRepositoryWriter) ClientOptions() repo.ClientOptions {
	opt := w.RepositoryWriter.ClientOptions()
	opt.EnableActions = w.enableActions

	return opt
}

func (r *countingRepository) ClientOptions() repo.ClientOptions {
	opt := r.Repository.ClientOptions()
	opt.EnableActions = r.enableActions

	return opt
}

func (r *countingRepository) NewWriter(ctx context.Context, opt repo.WriteSessionOptions) (context.Context, repo.RepositoryWriter, error) {
	r.newWriterCalls.Add(1)

	if r.failNewWriter.Load() {
		return nil, nil, errors.New("simulated offline repository")
	}

	wctx, w, err := r.Repository.NewWriter(ctx, opt)
	if err != nil {
		return nil, nil, err
	}

	return wctx, &countingRepositoryWriter{
		RepositoryWriter: w,
		enableActions:    r.enableActions,
	}, nil
}

type nopTaskController struct{}

func (nopTaskController) CurrentTaskID() string                         { return "task-test" }
func (nopTaskController) OnCancel(context.CancelFunc)                   {}
func (nopTaskController) ReportCounters(map[string]uitask.CounterValue) {}
func (nopTaskController) ReportProgressInfo(string)                     {}

func TestSourceManagerBeforeSnapshotRootAbortsBeforeWriteSession(t *testing.T) {
	t.Setenv("KOPIA_SERVER_TEST_ACTION_HELPER", "1")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0o600))

	si := env.LocalPathSourceInfo(srcDir)
	recordFile := filepath.Join(testutil.TempDirectory(t), "offline.log")

	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeSnapshotRoot: serverTestActionCommand("essential", "--record="+recordFile, "--fail"),
			AfterSnapshotRoot:  serverTestActionCommand("essential", "--record="+recordFile),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	crep := &countingRepository{
		Repository:    env.Repository,
		enableActions: true,
	}

	sm := newSourceManager(si, nil, crep)
	sm.refreshStatus(ctx)

	// Simulate repository backend going offline after policy was cached.
	crep.failNewWriter.Store(true)

	var res notifydata.ManifestWithError

	err := sm.snapshotInternal(ctx, nopTaskController{}, &res)
	require.ErrorContains(t, err, "essential action failed")
	require.Zero(t, crep.newWriterCalls.Load(), "write session must not be opened when essential before-snapshot-root fails")

	data, rerr := os.ReadFile(recordFile)
	require.NoError(t, rerr)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 1, "after-snapshot-root must not run when before-snapshot-root fails")
	require.True(t, strings.HasPrefix(lines[0], "before-snapshot-root|"))

	// An optional before-snapshot-root failure must not abort snapshot creation.
	crep.failNewWriter.Store(false)
	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeSnapshotRoot: serverTestActionCommand("optional", "--fail"),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))
	require.NoError(t, env.Repository.Refresh(ctx))
	sm.refreshStatus(ctx)

	require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))
	require.Equal(t, int32(1), crep.newWriterCalls.Load())
}

func TestSourceManagerRootActionsRunOnceAndCleanupOnWriteFailure(t *testing.T) {
	t.Setenv("KOPIA_SERVER_TEST_ACTION_HELPER", "1")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0o600))

	si := env.LocalPathSourceInfo(srcDir)
	recordFile := filepath.Join(testutil.TempDirectory(t), "once.log")

	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeSnapshotRoot: serverTestActionCommand("essential", "--record="+recordFile),
			AfterSnapshotRoot:  serverTestActionCommand("essential", "--record="+recordFile),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	crep := &countingRepository{
		Repository:    env.Repository,
		enableActions: true,
	}

	sm := newSourceManager(si, nil, crep)
	sm.refreshStatus(ctx)

	var res notifydata.ManifestWithError
	require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))
	require.Equal(t, int32(1), crep.newWriterCalls.Load())

	data, rerr := os.ReadFile(recordFile)
	require.NoError(t, rerr)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2, "before-snapshot-root and after-snapshot-root must each execute once")

	before1 := strings.Split(lines[0], "|")
	after1 := strings.Split(lines[1], "|")

	require.Equal(t, "before-snapshot-root", before1[0])
	require.Equal(t, "after-snapshot-root", after1[0])
	require.Equal(t, before1[1], after1[1])

	// When WriteSession fails after before-snapshot-root succeeded, after-snapshot-root must still run.
	crep.failNewWriter.Store(true)

	err := sm.snapshotInternal(ctx, nopTaskController{}, &res)
	require.ErrorContains(t, err, "simulated offline repository")
	require.Equal(t, int32(2), crep.newWriterCalls.Load())

	data, rerr = os.ReadFile(recordFile)
	require.NoError(t, rerr)

	lines = strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 4)

	before2 := strings.Split(lines[2], "|")
	after2 := strings.Split(lines[3], "|")

	require.Equal(t, "before-snapshot-root", before2[0])
	require.Equal(t, "after-snapshot-root", after2[0])
	require.Equal(t, before2[1], after2[1])
	require.NotEqual(t, before1[1], before2[1])
}

func TestSourceManagerBeforeSnapshotRootMountsOrRedirectsMissingSource(t *testing.T) {
	t.Setenv("KOPIA_SERVER_TEST_ACTION_HELPER", "1")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	crep := &countingRepository{
		Repository:    env.Repository,
		enableActions: true,
	}

	t.Run("mount-in-place", func(t *testing.T) {
		mountPath := filepath.Join(testutil.TempDirectory(t), "mounted-source")
		si := env.LocalPathSourceInfo(mountPath)

		require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
			Actions: policy.ActionsPolicy{
				BeforeSnapshotRoot: serverTestActionCommand("essential", "--create-source="+mountPath),
				AfterSnapshotRoot:  serverTestActionCommand("essential", "--remove-source="+mountPath),
			},
		}))
		require.NoError(t, env.RepositoryWriter.Flush(ctx))
		require.NoError(t, env.Repository.Refresh(ctx))

		sm := newSourceManager(si, nil, crep)
		sm.refreshStatus(ctx)

		var res notifydata.ManifestWithError
		require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))
		require.NoDirExists(t, mountPath, "after-snapshot-root should clean up mounted directory")
		require.Equal(t, int32(1), res.Manifest.Stats.TotalFileCount)
		require.Equal(t, int64(len("mounted-data")), res.Manifest.Stats.TotalFileSize)
	})

	t.Run("redirect-missing-source", func(t *testing.T) {
		logicalPath := filepath.Join(testutil.TempDirectory(t), "unmounted-logical")
		redirectDir := testutil.TempDirectory(t)
		require.NoError(t, os.WriteFile(filepath.Join(redirectDir, "snap.txt"), []byte("snap-data"), 0o600))

		si := env.LocalPathSourceInfo(logicalPath)

		require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
			Actions: policy.ActionsPolicy{
				BeforeSnapshotRoot: serverTestActionCommand("essential", "--redirect="+redirectDir),
			},
		}))
		require.NoError(t, env.RepositoryWriter.Flush(ctx))
		require.NoError(t, env.Repository.Refresh(ctx))

		sm := newSourceManager(si, nil, crep)
		sm.refreshStatus(ctx)

		var res notifydata.ManifestWithError
		require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))
		require.Equal(t, int32(1), res.Manifest.Stats.TotalFileCount)
	})
}

func TestSourceManagerUncachedRootActionFallsBackToUploader(t *testing.T) {
	t.Setenv("KOPIA_SERVER_TEST_ACTION_HELPER", "1")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0o600))

	si := env.LocalPathSourceInfo(srcDir)

	crep := &countingRepository{
		Repository:    env.Repository,
		enableActions: true,
	}

	sm := newSourceManager(si, nil, crep)
	sm.refreshStatus(ctx)

	// Set policy after refreshStatus so sourceManager has no cached root actions.
	recordFile := filepath.Join(testutil.TempDirectory(t), "uncached.log")
	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeSnapshotRoot: serverTestActionCommand("essential", "--record="+recordFile),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	var res notifydata.ManifestWithError
	require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))

	data, rerr := os.ReadFile(recordFile)
	require.NoError(t, rerr)
	require.True(t, strings.HasPrefix(strings.TrimSpace(string(data)), "before-snapshot-root|"))
}

func TestSourceManagerSingleFileSourceSkipsRootActions(t *testing.T) {
	t.Setenv("KOPIA_SERVER_TEST_ACTION_HELPER", "1")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	filePath := filepath.Join(testutil.TempDirectory(t), "single.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("single-file"), 0o600))

	si := env.LocalPathSourceInfo(filePath)
	recordFile := filepath.Join(testutil.TempDirectory(t), "single-file.log")

	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, policy.GlobalPolicySourceInfo, &policy.Policy{
		Actions: policy.ActionsPolicy{
			BeforeSnapshotRoot: serverTestActionCommand("essential", "--record="+recordFile, "--fail"),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	crep := &countingRepository{
		Repository:    env.Repository,
		enableActions: true,
	}

	sm := newSourceManager(si, nil, crep)
	sm.refreshStatus(ctx)

	var res notifydata.ManifestWithError
	require.NoError(t, sm.snapshotInternal(ctx, nopTaskController{}, &res))
	require.NoFileExists(t, recordFile)
}
