// +build darwin,amd64 linux,amd64

package robustness

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/testenv"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := 4096
	numFiles := 10000

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

func TestOneLargeFile(t *testing.T) {
	fileSize := 40 * 1024 * 1024
	numFiles := 1

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	fileSize := 4096
	numFiles := 1000
	filesPerWrite := 10
	actionRepeats := numFiles / filesPerWrite

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(15),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

func TestRandomizedSmall(t *testing.T) {
	st := clock.Now()

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

	for clock.Since(st) <= *randomizedTestDur {
		err := eng.RandomAction(opts)
		if errors.Is(err, robustness.ErrNoOp) {
			t.Log("Random action resulted in no-op")

			err = nil
		}

		testenv.AssertNoError(t, err)
	}
}
