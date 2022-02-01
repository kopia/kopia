package cli_test

import (
	"testing"

	"github.com/alecthomas/kingpin"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) setupInMemoryRepo(t *testing.T) *testenv.CLITest {
	t.Helper()

	runner := testenv.NewInProcRunner(t)
	runner.CustomizeApp = func(a *cli.App, kp *kingpin.Application) {
		a.AddStorageProvider(cli.StorageProvider{
			Name:        "in-memory",
			Description: "in-memory storage backend",
			NewFlags:    func() cli.StorageFlags { return &cli.StorageInMemoryFlags{} },
		})
	}

	env := testenv.NewCLITest(t, s.formatFlags, runner)
	st := repotesting.NewReconnectableStorage(t, blobtesting.NewVersionedMapStorage(nil))

	env.RunAndExpectSuccess(t, "repo", "create", "in-memory", "--uuid",
		st.ConnectionInfo().Config.(*repotesting.ReconnectableStorageOptions).UUID)

	return env
}

func (s *formatSpecificTestSuite) TestRepositorySetParameters(t *testing.T) {
	env := s.setupInMemoryRepo(t)
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     20 MiB")

	if s.formatVersion == content.FormatVersion1 {
		require.Contains(t, out, "Format version:      1")
	} else {
		require.Contains(t, out, "Format version:      2")
	}

	// failure cases
	env.RunAndExpectFailure(t, "repository", "set-parameters")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--index-version=33")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=9")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=121")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--index-version=2", "--max-pack-size-mb=33")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     33 MiB")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--max-pack-size-mb=44")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     44 MiB")
}

func (s *formatSpecificTestSuite) TestRepositorySetParametersUpgrade(t *testing.T) {
	env := s.setupInMemoryRepo(t)
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     20 MiB")

	if s.formatVersion == content.FormatVersion1 {
		require.Contains(t, out, "Format version:      1")
		require.Contains(t, out, "Epoch Manager:       disabled")
		env.RunAndExpectFailure(t, "index", "epoch", "list")
	} else {
		require.Contains(t, out, "Format version:      2")
		require.Contains(t, out, "Epoch Manager:       enabled")
		env.RunAndExpectSuccess(t, "index", "epoch", "list")
	}

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
	require.Contains(t, out, "Format version:      2")
	require.Contains(t, out, "Epoch cleanup margin:    23h0m0s")
	require.Contains(t, out, "Epoch advance on:        22 blobs or 77 MiB, minimum 3h0m0s")
	require.Contains(t, out, "Epoch checkpoint every:  9 epochs")

	env.RunAndExpectSuccess(t, "index", "epoch", "list")
}
