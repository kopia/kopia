package endtoend_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/tests/testenv"
)

func TestFilesystemFlat(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--flat")
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))

	entries, err := os.ReadDir(e.RepoDir)
	require.NoError(t, err)

	// make sure there are no subdirectories in the repo.
	for _, ent := range entries {
		t.Logf("found %v %v", ent.Name(), ent.IsDir())
		require.False(t, ent.IsDir())
	}
}

func TestFilesystemRequiresAbsolutePaths(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectFailure(t, "repo", "create", "filesystem", "--path", "./relative-path")
}

func TestFilesystemSupportsTildeToReferToHome(t *testing.T) {
	t.Parallel()

	home, _ := os.UserHomeDir()
	if home == "" {
		t.Skip("home directory not available")
	}

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	subdir := "repo-" + uuid.NewString()
	fullPath := filepath.Join(home, subdir)

	defer os.RemoveAll(fullPath)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path=~/"+subdir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	if _, err := os.Stat(filepath.Join(fullPath, "kopia.repository.f")); err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestReconnect(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "repo", "status")
}

func TestReconnectUsingToken(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	lines := e.RunAndExpectSuccess(t, "repo", "status", "-t", "-s")
	prefix := "$ kopia "

	var reconnectArgs []string

	// look for output line containing the prefix - this will be our reconnect command
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			reconnectArgs = strings.Split(strings.TrimPrefix(l, prefix), " ")
		}
	}

	if reconnectArgs == nil {
		t.Fatalf("can't find reonnect command in kopia repo status output")
	}

	e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, reconnectArgs...)
	e.RunAndExpectSuccess(t, "repo", "status")
}

func TestRepoConnectKeyDerivationAlgorithm(t *testing.T) {
	t.Parallel()
	for _, algorithm := range format.SupportedFormatBlobKeyDerivationAlgorithms() {
		runner := testenv.NewInProcRunner(t)
		e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

		e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--format-block-key-derivation-algorithm", algorithm)

		e.RunAndExpectSuccess(t, "repo", "disconnect")
		e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

		kopiaRepoPath := filepath.Join(e.RepoDir, "kopia.repository.f")
		dat, err := os.ReadFile(kopiaRepoPath)
		require.NoError(t, err)
		var repoJSON format.KopiaRepositoryJSON
		json.Unmarshal(dat, &repoJSON)
		require.Equal(t, repoJSON.KeyDerivationAlgorithm, algorithm)
	}
}

func TestRepoConnectBadKeyDerivationAlgorithm(t *testing.T) {
	t.Parallel()
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--format-block-key-derivation-algorithm", format.DefaultKeyDerivationAlgorithm)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	kopiaRepoPath := filepath.Join(e.RepoDir, "kopia.repository.f")
	dat, err := os.ReadFile(kopiaRepoPath)
	require.NoError(t, err)
	var repoJSON format.KopiaRepositoryJSON
	json.Unmarshal(dat, &repoJSON)

	repoJSON.KeyDerivationAlgorithm = "badalgorithm"

	jsonString, _ := json.Marshal(repoJSON)
	os.WriteFile(kopiaRepoPath, jsonString, os.ModePerm)

	e.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e.RepoDir)
}
