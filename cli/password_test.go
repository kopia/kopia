package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func TestPasswordFile(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "repo", "disconnect")

	passwordFile := filepath.Join(t.TempDir(), "password.txt")
	require.NoError(t, os.WriteFile(passwordFile, []byte(testenv.TestRepoPassword+"\n"), 0o600))

	delete(env.Environment, "KOPIA_PASSWORD")
	env.Environment["KOPIA_PASSWORD_FILE"] = passwordFile

	env.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "repo", "status")
}

func TestPasswordFile_Invalid(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "repo", "disconnect")

	emptyPasswordFile := filepath.Join(t.TempDir(), "empty.txt")
	require.NoError(t, os.WriteFile(emptyPasswordFile, nil, 0o600))

	delete(env.Environment, "KOPIA_PASSWORD")

	env.Environment["KOPIA_PASSWORD_FILE"] = filepath.Join(t.TempDir(), "no-such-file.txt")
	env.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", env.RepoDir)

	env.Environment["KOPIA_PASSWORD_FILE"] = emptyPasswordFile
	env.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", env.RepoDir)
}
