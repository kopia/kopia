package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) setupInMemoryRepo(t *testing.T) *testenv.CLITest {
	t.Helper()

	runner := testenv.NewInProcRunner(t)
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

func (s *formatSpecificTestSuite) TestRepositorySetParametersRetention(t *testing.T) {
	env := s.setupInMemoryRepo(t)

	// set retention
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", blob.Compliance.String(),
		"--retention-period", "24h")

	out := env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     COMPLIANCE")
	require.Contains(t, out, "Blob retention period:   24h0m0s")

	// update retention
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", blob.Governance.String(),
		"--retention-period", "24h1m")

	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     GOVERNANCE")
	require.Contains(t, out, "Blob retention period:   24h1m0s")

	// clear retention settings
	_, out = env.RunAndExpectSuccessWithErrOut(t, "repository", "set-parameters", "--retention-mode", "none")
	require.Contains(t, out, "disabling blob retention")

	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.NotContains(t, out, "Blob retention mode")
	require.NotContains(t, out, "Blob retention period")

	// invalid retention settings
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--retention-mode", "invalid-mode")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--retention-mode", "COMPLIANCE", "--retention-period", "0h")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--retention-mode", "COMPLIANCE", "--retention-period", "6h") // less than 24hr

	// set retention again after clear
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", "COMPLIANCE", "--retention-period", "24h")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     COMPLIANCE")
	require.Contains(t, out, "Blob retention period:   24h0m0s")

	// update without period
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-period", "25h")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     COMPLIANCE")
	require.Contains(t, out, "Blob retention period:   25h0m0s")

	// update without mode
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", "GOVERNANCE")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     GOVERNANCE")
	require.Contains(t, out, "Blob retention period:   25h0m0s")

	// update retention (use days, weeks, nanoseconds)
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", blob.Compliance.String(),
		"--retention-period", "1w2d6h3ns")

	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     COMPLIANCE")
	require.Contains(t, out, "Blob retention period:   222h0m0.000000003s")
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
