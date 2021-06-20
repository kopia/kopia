package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositorySetParameters(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     20 MiB")
	require.Contains(t, out, "Index Format:        v1")

	// failure cases
	env.RunAndExpectFailure(t, "repository", "set-params")
	env.RunAndExpectFailure(t, "repository", "set-params", "--index-version=33")
	env.RunAndExpectFailure(t, "repository", "set-params", "--max-pack-size-mb=9")
	env.RunAndExpectFailure(t, "repository", "set-params", "--max-pack-size-mb=121")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--index-version=2", "--max-pack-size-mb=33")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     33 MiB")
	require.Contains(t, out, "Index Format:        v2")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--max-pack-size-mb=44")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     44 MiB")
}
