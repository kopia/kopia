package cli_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
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
	require.Contains(t, out, "Max pack length:     21 MB")
	require.Contains(t, out, fmt.Sprintf("Format version:      %d", s.formatVersion))

	_, out = env.RunAndExpectSuccessWithErrOut(t, "repository", "set-parameters")
	require.Contains(t, out, "no changes")

	// failure cases
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--index-version=33")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=9")
	env.RunAndExpectFailure(t, "repository", "set-parameters", "--max-pack-size-mb=121")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--index-version=2", "--max-pack-size-mb=33")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     34.6 MB")

	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--max-pack-size-mb=44")
	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Max pack length:     46.1 MB")
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

	// 2nd time also succeeds but disabling is skipped due to already being disabled. !anyChanges returns no error.
	_, out = env.RunAndExpectSuccessWithErrOut(t, "repository", "set-parameters", "--retention-mode", "none")
	require.Contains(t, out, "no changes")

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

	// update retention (use days)
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--retention-mode", blob.Compliance.String(),
		"--retention-period", "7d")

	out = env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Blob retention mode:     COMPLIANCE")
	require.Contains(t, out, "Blob retention period:   168h0m0s")
}

func (s *formatSpecificTestSuite) TestRepositorySetParametersUpgrade(t *testing.T) {
	env := s.setupInMemoryRepo(t)
	out := env.RunAndExpectSuccess(t, "repository", "status")

	// default values
	require.Contains(t, out, "Max pack length:     21 MB")

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		require.Contains(t, out, "Epoch Manager:       disabled")
		env.RunAndExpectFailure(t, "index", "epoch", "list")
	case format.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		require.Contains(t, out, "Epoch Manager:       enabled")
		env.RunAndExpectSuccess(t, "index", "epoch", "list")
	default:
		require.Contains(t, out, "Format version:      3")
		require.Contains(t, out, "Epoch Manager:       enabled")
		env.RunAndExpectSuccess(t, "index", "epoch", "list")
	}

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	{
		cmd := []string{
			"repository", "upgrade", "begin",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s",
		}

		// You can only upgrade when you are not already upgraded
		if s.formatVersion < format.MaxFormatVersion {
			env.RunAndExpectSuccess(t, cmd...)
		} else {
			_, stderr := env.RunAndExpectSuccessWithErrOut(t, cmd...)
			require.Contains(t, stderr, "Repository format is already upto date.")
		}
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
	require.Contains(t, out, "Format version:      3")
	require.Contains(t, out, "Epoch cleanup margin:    23h0m0s")
	require.Contains(t, out, "Epoch advance on:        22 blobs or 80.7 MB, minimum 3h0m0s")
	require.Contains(t, out, "Epoch checkpoint every:  9 epochs")

	env.RunAndExpectSuccess(t, "index", "epoch", "list")
}

// TestRepositorySetParametersDowngrade test that a repository cannot be downgraded by using `set-parameters`.
func (s *formatSpecificTestSuite) TestRepositorySetParametersDowngrade(t *testing.T) {
	env := s.setupInMemoryRepo(t)

	// checkStatusForVersion is a function with stanzas to check that the repository has the expected version.
	// 	its saved into a variable to prevent repetition and enforce that nothing has changed between invocations
	//  if `set-parameters`
	checkStatusForVersion := func() {
		out := env.RunAndExpectSuccess(t, "repository", "status")

		// default values
		require.Contains(t, out, "Max pack length:     21 MB")

		switch s.formatVersion {
		case format.FormatVersion1:
			require.Contains(t, out, "Format version:      1")
			require.Contains(t, out, "Epoch Manager:       disabled")
			env.RunAndExpectFailure(t, "index", "epoch", "list")
			// setting the current version again is ok
			_, out = env.RunAndExpectSuccessWithErrOut(t, "repository", "set-parameters", "--index-version=1")
			require.Contains(t, out, "no changes")
		case format.FormatVersion2:
			require.Contains(t, out, "Format version:      2")
			require.Contains(t, out, "Epoch Manager:       enabled")
			env.RunAndExpectSuccess(t, "index", "epoch", "list")
			_, out = env.RunAndExpectFailure(t, "repository", "set-parameters", "--index-version=1")
			require.Contains(t, out, "index format version can only be upgraded")
		default:
			require.Contains(t, out, "Format version:      3")
			require.Contains(t, out, "Epoch Manager:       enabled")
			env.RunAndExpectSuccess(t, "index", "epoch", "list")
			_, out = env.RunAndExpectFailure(t, "repository", "set-parameters", "--index-version=1")
			require.Contains(t, out, "index format version can only be upgraded")
		}
	}

	checkStatusForVersion()

	checkStatusForVersion()

	// run basic check to ensure that an upgrade can still be performed as expected
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--upgrade")

	out := env.RunAndExpectSuccess(t, "repository", "status")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
}

func (s *formatSpecificTestSuite) TestRepositorySetParametersRequiredFeatures(t *testing.T) {
	env := s.setupInMemoryRepo(t)

	env.RunAndExpectSuccess(t, "repository", "status")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--add-required-feature", "no-such-feature")
	env.RunAndExpectFailure(t, "repository", "status")
	env.RunAndExpectSuccess(t, "repository", "status", "--ignore-missing-required-features")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--remove-required-feature", "no-such-feature", "--ignore-missing-required-features")
	env.RunAndExpectSuccess(t, "repository", "status")

	// now require a feature but with a warning
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--add-required-feature", "no-such-feature", "--warn-on-missing-required-feature")
	env.RunAndExpectSuccess(t, "repository", "status")
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--remove-required-feature", "no-such-feature")
}

func (s *formatSpecificTestSuite) TestRepositorySetParametersRequiredFeatures_ServerMode(t *testing.T) {
	env := s.setupInMemoryRepo(t)

	env.RunAndExpectSuccess(t, "repo", "set-client", "--repository-format-cache-duration=1s")

	var sp testutil.ServerParameters

	snapDir := testutil.TempDirectory(t)

	// create a snapshot that will be created every second
	env.RunAndExpectSuccess(t, "snapshot", "create", snapDir)
	env.RunAndExpectSuccess(t, "policy", "set", "--snapshot-interval=1s", snapDir)

	wait, _ := env.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	// now introduce required parameters while the server is running
	env.RunAndExpectSuccess(t, "repository", "set-parameters", "--add-required-feature", "no-such-feature")

	// we are aggressively creating snapshots every second,
	// the server will soon notice the new required feature and shut down.
	require.ErrorContains(t, wait(), "no-such-feature")
}
