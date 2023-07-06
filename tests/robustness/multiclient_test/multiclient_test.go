//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package multiclienttest

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

const defaultTestDur = 5 * time.Minute

var randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")

func TestManySmallFiles(t *testing.T) {
	const (
		fileSize   = 4096
		numFiles   = 10000
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
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
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
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
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
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(15),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		require.NoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
		require.NoError(t, err)
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)
	th.RunN(ctx, t, numClients, f)
}

func TestRandomizedSmall(t *testing.T) {
	const numClients = 4

	st := timetrack.StartTimer()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotDirActionKey):              strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.IOLimitPerWriteAction:    fmt.Sprintf("%d", 512*1024*1024),
			fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(100),
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			fiofilewriter.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	f := func(ctx context.Context, t *testing.T) { //nolint:thelper
		err := tryRestoreIntoDataDirectory(ctx, t)
		err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
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
