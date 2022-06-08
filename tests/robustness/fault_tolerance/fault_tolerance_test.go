//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package robustness

import (
	"strconv"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/stretchr/testify/require"
)

func TestSnapshotFix(t *testing.T) {
	const (
		fileSize = 409
		numFiles = 100
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	opts := map[string]string{
		string(engine.DeleteRandomBlobActionKey): strconv.Itoa(1),
	}

	// current state: a test repo is available in /test-repo/robustness-data
	// test main connects to test-repo on filesystem, test -repo = SUT
	// restores a snapshot from test-repo into /tmp/

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	// perform changes to CF volume data, snapshot
	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	// list blobs in SUT,
	// kopia blob list
	// no way to do ^ as of now
	// delete random blobs - decide the number, start with 2
	// kopia blob delete <blob ID> --advanced-commands=enabled
	_, err = eng.ExecAction(ctx, engine.DeleteRandomBlobActionKey, opts)
	require.NoError(t, err)

	// try to restore the latest snapshot
	// this should error out
	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	require.Error(t, err)

}
