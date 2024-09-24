package cli_test

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotVerify(t *testing.T) {
	srcDir1 := testutil.TempDirectory(t)

	runner := testenv.NewInProcRunner(t)
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	var intactMan, corruptMan1, corruptMan2 snapshot.Manifest

	// Write a file, create a new snapshot.
	intactFileName := "intact"
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, intactFileName), 1, bytes.Repeat([]byte{1, 2, 3}, 100))
	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "create", srcDir1, "--json"), &intactMan)

	// Write a new file not present in the previous snapshot.
	corruptFileName1 := "corrupt1"
	pattern1 := []byte{1, 2, 4}
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, corruptFileName1), 1, bytes.Repeat(pattern1, 100))

	// Create a snapshot including the new file.
	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "create", srcDir1, "--json"), &corruptMan1)

	// Write a new file not present in the previous two snapshots. Use a data pattern
	// distinct from the previous file to prevent dedup.
	corruptFileName2 := "corrupt2"
	pattern2 := []byte{1, 2, 5}
	mustWriteFileWithRepeatedData(t, filepath.Join(srcDir1, corruptFileName2), 1, bytes.Repeat(pattern2, 100))

	// Create a snapshot including the new file.
	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "create", srcDir1, "--json"), &corruptMan2)

	// Corrupt the blobs containing the contents associated with the files to be corrupted.
	fileMap := mustGetFileMap(t, env, corruptMan2.RootObjectID())
	forgetContents(t, env, fileMap[corruptFileName1].ObjectID.String())
	forgetContents(t, env, fileMap[corruptFileName2].ObjectID.String())

	// Verifying everything is expected to fail.
	env.RunAndExpectFailure(t, "snapshot", "verify")

	// Verifying the untouched snapshot is expected to succeed.
	env.RunAndExpectSuccess(t, "snapshot", "verify", string(intactMan.ID))

	// Verifying the corrupted snapshot is expected to fail.
	env.RunAndExpectFailure(t, "snapshot", "verify", string(corruptMan1.ID))

	// Verifying the corrupted snapshot is expected to fail.
	env.RunAndExpectFailure(t, "snapshot", "verify", string(corruptMan2.ID))

	// Find one matching error corresponding to the single corrupted contents.
	_, stderr, err := env.Run(t, true, "snapshot", "verify", "--max-errors", "3", string(corruptMan1.ID))
	require.Error(t, err)
	assert.Equal(t, 1, strings.Count(strings.Join(stderr, "\n"), "error processing"))

	// Find two matching errors in the verify output, corresponding to each
	// of the two corrupted contents.
	_, stderr, err = env.Run(t, true, "snapshot", "verify", "--max-errors", "3", string(corruptMan2.ID))
	require.Error(t, err)
	assert.Equal(t, 2, strings.Count(strings.Join(stderr, "\n"), "error processing"))

	// Requesting a snapshot verify of a non-existent manifest ID results in error.
	env.RunAndExpectFailure(t, "snapshot", "verify", "not-a-manifest-id")
}
