//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package multiclienttest

import (
	"context"
	"errors"
	"flag"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
)

const (
	defaultTestDur           = 5 * time.Minute
	deleteContentsPercentage = 50
)

var randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")

func TestManySmallFiles(t *testing.T) {
	const (
		fileSize    = 4096
		numFiles    = 10000
		numClients  = 4
		maxDirDepth = 1
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(maxDirDepth),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}
	deleteDirOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:             strconv.Itoa(maxDirDepth),
		fiofilewriter.DeletePercentOfContentsField: strconv.Itoa(deleteContentsPercentage),
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		require.NoError(t, err)

		tryDeleteAction(ctx, t, engine.DeleteRandomSubdirectoryActionKey, deleteDirOpts)

		tryDeleteAction(ctx, t, engine.DeleteDirectoryContentsActionKey, deleteDirOpts)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		require.NoError(t, err)
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

func TestOneLargeFile(t *testing.T) {
	const (
		fileSize   = 40 * 1024 * 1024
		numFiles   = 1
		numClients = 4
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		require.NoError(t, err)
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	const (
		fileSize      = 4096
		numFiles      = 1000
		filesPerWrite = 10
		actionRepeats = numFiles / filesPerWrite
		numClients    = 4
		maxDirDepth   = 15
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(maxDirDepth),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}
	deleteDirOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:             strconv.Itoa(maxDirDepth),
		fiofilewriter.DeletePercentOfContentsField: strconv.Itoa(deleteContentsPercentage),
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		require.NoError(t, err)

		tryDeleteAction(ctx, t, engine.DeleteRandomSubdirectoryActionKey, deleteDirOpts)

		tryDeleteAction(ctx, t, engine.DeleteDirectoryContentsActionKey, deleteDirOpts)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		require.NoError(t, err)
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

func TestRandomizedSmall(t *testing.T) {
	const numClients = 4

	st := timetrack.StartTimer()

	maxDirDepth := 3

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotDirActionKey):              strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(2),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
			string(engine.DeleteDirectoryContentsActionKey):  strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.IOLimitPerWriteAction:    strconv.Itoa(512 * 1024 * 1024),
			fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(100),
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			fiofilewriter.MaxDirDepthField:         strconv.Itoa(maxDirDepth),
		},
		engine.DeleteDirectoryContentsActionKey: map[string]string{
			fiofilewriter.DeletePercentOfContentsField: strconv.Itoa(deleteContentsPercentage),
		},
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		require.NoError(t, err)

		//nolint:forbidigo
		for st.Elapsed() <= *randomizedTestDur {
			err := tryRandomAction(ctx, t, opts)
			require.NoError(t, err)
		}
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

func TestMaintenanceAction(t *testing.T) {
	t.Log("running maintenance directly on the repository under test")

	// bypass the server to directly run maintenance on the repository
	// under test.
	// It launches a kopia process that directly accesses the repository
	// under test using the repo configuration for the server. The
	// server is concurrently running, since the framework starts
	// the server at the beginning of an execution of the framework.
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	_, err := eng.ExecAction(ctx, engine.GCActionKey, nil)

	require.NoError(t, err)
}

func TestDeleteRandomSnapshotAction(t *testing.T) {
	const numClients = 1

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		tryDeleteAction(ctx, t, engine.DeleteRandomSnapshotActionKey, nil)
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

// tryRestoreIntoDataDirectory runs eng.ExecAction on the given parameters and masks no-op errors.
func tryRestoreIntoDataDirectory(ctx context.Context, t *testing.T) error { //nolint:thelper
	_, err := eng.ExecAction(ctx, engine.RestoreIntoDataDirectoryActionKey, nil)
	if errors.Is(err, robustness.ErrNoOp) {
		t.Log("Action resulted in no-op")
		return nil
	}

	return err
}

// tryRandomAction runs eng.ExecAction on the given parameters and masks no-op errors.
func tryRandomAction(ctx context.Context, t *testing.T, opts engine.ActionOpts) error { //nolint:thelper
	err := eng.RandomAction(ctx, opts)
	if errors.Is(err, robustness.ErrNoOp) {
		t.Log("Random action resulted in no-op")
		return nil
	}

	return err
}

// tryDeleteAction runs the given delete action,
// delete-files or delete-random-subdirectory or delete-random-snapID
// with options and masks no-op errors, and asserts when called for any other action.
func tryDeleteAction(ctx context.Context, t *testing.T, action engine.ActionKey, actionOpts map[string]string) {
	t.Helper()
	eligibleActionsList := []engine.ActionKey{
		engine.DeleteDirectoryContentsActionKey,
		engine.DeleteRandomSubdirectoryActionKey,
		engine.DeleteRandomSnapshotActionKey,
	}
	require.Contains(t, eligibleActionsList, action)

	_, err := eng.ExecAction(ctx, action, actionOpts)
	// Ignore the dir-not-found error wrapped as no-op error.
	if errors.Is(err, robustness.ErrNoOp) {
		t.Logf("Delete action '%s' resulted in no-op", action)
		return
	}

	require.NoError(t, err)
}
