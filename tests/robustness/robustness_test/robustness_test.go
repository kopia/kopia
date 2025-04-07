//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package robustness

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
)

func TestManySmallFiles(t *testing.T) {
	const (
		fileSize = 4096
		numFiles = 10000
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
	require.NoError(t, err)
}

func TestOneLargeFile(t *testing.T) {
	const (
		fileSize = 40 * 1024 * 1024
		numFiles = 1
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	require.NoError(t, err)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	const (
		fileSize      = 4096
		numFiles      = 1000
		filesPerWrite = 10
		actionRepeats = numFiles / filesPerWrite
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(15),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	err = eng.CheckErrRecovery(ctx, err, engine.ActionOpts{})
	require.NoError(t, err)
}

func TestRandomizedSmall(t *testing.T) {
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
			fiofilewriter.IOLimitPerWriteAction:    strconv.Itoa(512 * 1024 * 1024),
			fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(100),
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			fiofilewriter.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	for st.Elapsed() <= *randomizedTestDur {
		err := eng.RandomAction(ctx, opts)
		if errors.Is(err, robustness.ErrNoOp) {
			t.Log("Random action resulted in no-op")

			err = nil
		}

		require.NoError(t, err)
	}
}
