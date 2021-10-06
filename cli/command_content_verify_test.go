package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestContentVerify(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	dir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file1.txt"), bytes.Repeat([]byte{1, 2, 3, 4, 5}, 15000), 0o600))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "content", "verify")
	env.RunAndExpectSuccess(t, "snapshot", "create", dir)
	env.RunAndExpectSuccess(t, "content", "verify", "--download-percent=30")

	// delete one of 'p' blobs.
	blobIDToDelete := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=p")[0], " ")[0]
	blobList := env.RunAndExpectSuccess(t, "blob", "list")
	t.Logf("blob list: %v", strings.Join(blobList, "\n"))
	env.RunAndExpectSuccess(t, "blob", "delete", blobIDToDelete)

	_, verifyStderr, err := env.Run(t, true, "content", "verify")
	require.Error(t, err)

	// this fails if not found
	mustGetLineContaining(t, verifyStderr, "missing blob "+blobIDToDelete)

	env.RunAndExpectFailure(t, "content", "verify", "--full")
}
