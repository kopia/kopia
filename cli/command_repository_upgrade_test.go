package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestRepositoryUpgrade(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	switch s.formatVersion {
	case content.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have been upgraded.")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	case content.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	default:
		require.Contains(t, out, "Format version:      3")
		env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s")
	}

	out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Format version:      3")
}

func (s *formatSpecificTestSuite) TestRepositoryUpgradeAdvanceNotice(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	switch s.formatVersion {
	case content.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, strings.Join(stderr, "\n"),
			"Repository upgrade advance notice has been set, you must come back and perform the upgrade")

		// verify that non-owner clients will fail to connect/upgrade
		env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "non-owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s")

		// until we drain, we would be able to see the upgrade status
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 1 -> 3")
		require.Contains(t, out, "Upgrade lock:        Unlocked")
		require.Contains(t, out, "Lock status:         Draining")

		// attempt to rollback the upgrade and restart
		env.RunAndExpectSuccess(t, "repository", "upgrade", "--force-rollback")
		env.RunAndExpectSuccess(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")

		// drain all clients
		_, stderr = env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--block-until-drain",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, stderr, "Successfully drained all repository clients, the lock has been fully-established now.")

		// verify that access is denied after we drain
		env.RunAndExpectFailure(t, "repository", "status", "--upgrade-no-block")

		// verify that owner clients can check status
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-owner-id", "owner")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 1 -> 3")
		require.Contains(t, out, "Upgrade lock:        Locked")
		require.Contains(t, out, "Lock status:         Fully Established")

		// finalize the upgrade
		_, stderr = env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")

		// verify that non-owner clients can resume access
		env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	case content.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, strings.Join(stderr, "\n"),
			"Repository upgrade advance notice has been set, you must come back and perform the upgrade")

		// verify that non-owner clients will fail to connect/upgrade
		env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "non-owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s")

		// until we drain, we would be able to see the upgrade status
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 2 -> 3")
		require.Contains(t, out, "Upgrade lock:        Unlocked")
		require.Contains(t, out, "Lock status:         Draining")

		// attempt to rollback the upgrade and restart
		env.RunAndExpectSuccess(t, "repository", "upgrade", "--force-rollback")
		env.RunAndExpectSuccess(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")

		// drain all clients
		_, stderr = env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--block-until-drain",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
		require.Contains(t, stderr, "Continuing to drain since advance notice has been set and we have been requested to block until then")
		require.Contains(t, stderr, "Successfully drained all repository clients, the lock has been fully-established now.")

		// verify that access is denied after we drain
		env.RunAndExpectFailure(t, "repository", "status", "--upgrade-no-block")

		// verify that owner clients can check status
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-owner-id", "owner")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 2 -> 3")
		require.Contains(t, out, "Upgrade lock:        Locked")
		require.Contains(t, out, "Lock status:         Fully Established")

		// finalize the upgrade
		_, stderr = env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")

		// verify that non-owner clients can resume access
		env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	default:
		require.Contains(t, out, "Format version:      3")
		env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--force",
			"--status-poll-interval", "1s",
			"--max-clock-drift", "1s",
			"--advance-notice", "30s")
	}

	out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Format version:      3")
}
