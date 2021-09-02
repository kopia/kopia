package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositorySetParameters(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--index-version=1")
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     20 MiB")
	require.Contains(t, out, "Index Format:        v1")

	// failure cases
	env.RunAndExpectFailure(t, "repository", "set-parameters")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--index-version=33")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=9")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=121")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--index-version=2", "--max-pack-size-mb=33")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     33 MiB")
	require.Contains(t, out, "Index Format:        v2")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--max-pack-size-mb=44")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     44 MiB")
}

func TestRepositorySetParametersUpgrade(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--index-version=1", "--no-enable-index-epochs")
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     20 MiB")
	require.Contains(t, out, "Index Format:        v1")
	require.Contains(t, out, "Epoch Manager:       disabled")

	env.RunAndExpectFailure(t, "index", "epoch", "list")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--upgrade")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--epoch-min-duration", "3h")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--epoch-cleanup-safety-margin", "23h")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--epoch-advance-on-size-mb", "77")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--epoch-advance-on-count", "22")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--epoch-checkpoint-frequency", "9")

	env.RunAndExpectFailure(t, "repository", "set-parameters", "--epoch-min-duration", "1s")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--epoch-refresh-frequency", "10h")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--epoch-checkpoint-frequency", "-10")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--epoch-cleanup-safety-margin", "10s")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--epoch-advance-on-count", "1")

	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Epoch cleanup margin:    23h0m0s")
	require.Contains(t, out, "Epoch advance on:        22 blobs or 77 MiB, minimum 3h0m0s")
	require.Contains(t, out, "Epoch checkpoint every:  9 epochs")

	env.RunAndExpectSuccess(t, "index", "epoch", "list")
}
