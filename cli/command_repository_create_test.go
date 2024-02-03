package cli_test

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/tests/testenv"

	"github.com/stretchr/testify/require"
)

func TestRepositoryCreateWithConfigFile(t *testing.T) {
	env := testenv.NewCLITest(t, nil, testenv.NewInProcRunner(t))

	_, stderr := env.RunAndExpectFailure(t, "repo", "create", "from-config", "--file", path.Join(env.ConfigDir, "does_not_exist.config"))
	require.Contains(t, stderr, "can't connect to storage: one of --token-file, --token-stdin or --token must be provided")

	_, stderr = env.RunAndExpectFailure(t, "repo", "connect", "from-config")
	require.Contains(t, stderr, "can't connect to storage: one of --file, --token-file, --token-stdin or --token must be provided")

	_, stderr = env.RunAndExpectFailure(t, "repo", "create", "from-config", "--token", "bad-token")
	require.Contains(t, stderr, "can't connect to storage: invalid token: unable to decode token")

	storageCfgFName := path.Join(env.ConfigDir, "storage-config.json")
	ci := blob.ConnectionInfo{
		Type:   "filesystem",
		Config: filesystem.Options{Path: env.RepoDir},
	}
	token, err := repo.EncodeToken("12345678", ci)
	require.NoError(t, err)

	// expect failure before writing to file
	_, stderr = env.RunAndExpectFailure(t, "repo", "create", "from-config", "--token-file", storageCfgFName)
	require.Contains(t, strings.Join(stderr, "\n"), "can't connect to storage: unable to open token file")

	require.NoError(t, os.WriteFile(storageCfgFName, []byte(token), 0o600))

	defer os.Remove(storageCfgFName) //nolint:errcheck,gosec

	env.RunAndExpectSuccess(t, "repo", "create", "from-config", "--token-file", storageCfgFName)
}

func TestRepositoryCreateWithConfigFromStdin(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	env := testenv.NewCLITest(t, nil, runner)

	ci := blob.ConnectionInfo{
		Type:   "filesystem",
		Config: filesystem.Options{Path: env.RepoDir},
	}
	token, err := repo.EncodeToken("12345678", ci)
	require.NoError(t, err)

	// set stdin
	runner.SetNextStdin(strings.NewReader(token))

	env.RunAndExpectSuccess(t, "repo", "create", "from-config", "--token-stdin")
}
