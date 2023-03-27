package cli_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestRepositoryUpgrade(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have been upgraded.")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	case format.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	default:
		require.Contains(t, out, "Format version:      3")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository format is already upto date.")
	}

	out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Format version:      3")
}

func (s *formatSpecificTestSuite) TestRepositoryCorruptedUpgrade(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		// run upgrade first with commit-mode set to never.  this leaves the lock and new index intact so that
		// the file can be corrupted with "TweakFile".
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--commit-mode", "never",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have been upgraded.")
		require.Contains(t, stderr, "Commit mode is set to 'never'.  Skipping commit.")
		require.Contains(t, stderr, "index validation succeeded")
		env.TweakFile(t, env.RepoDir, "x*/*/*.f")
		// then re-run the upgrade with the corrupted index.  This should fail on index validation.
		_, stderr = env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner")
		require.Regexp(t, "failed to load index entries for new index: failed to load index blob with BlobID", stderr)
	case format.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
	default:
		require.Contains(t, out, "Format version:      3")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository format is already upto date.")
	}
}

func (s *formatSpecificTestSuite) TestRepositoryUpgradeCommitNever(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	stdout := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, stdout, "Format version:      1")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "never",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have been upgraded.")
		require.Contains(t, stderr, "Commit mode is set to 'never'.  Skipping commit.")

		_, stderr = env.RunAndExpectFailure(t, "repository", "status", "--upgrade-no-block")
		require.Contains(t, stderr, "failed to open repository: repository upgrade in progress")
	case format.FormatVersion2:
		require.Contains(t, stdout, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "never",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
		require.Contains(t, stderr, "Commit mode is set to 'never'.  Skipping commit.")

		_, stderr = env.RunAndExpectFailure(t, "repository", "status", "--upgrade-no-block")
		require.Contains(t, stderr, "failed to open repository: repository upgrade in progress")
	default:
		require.Contains(t, stdout, "Format version:      3")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "never",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository format is already upto date.")

		env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	}
}

func (s *formatSpecificTestSuite) TestRepositoryUpgradeCommitAlways(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "always",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have been upgraded.")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	case format.FormatVersion2:
		require.Contains(t, out, "Format version:      2")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "always",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository indices have already been migrated to the epoch format, no need to drain other clients")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")
	default:
		require.Contains(t, out, "Format version:      3")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--commit-mode", "always",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository format is already upto date.")
	}

	out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Format version:      3")
}

func lockRepositoryForUpgrade(t *testing.T, env *testenv.CLITest) {
	t.Helper()

	t.Log("Placing upgrade lock ...")
	env.RunAndExpectSuccess(t, "repository", "upgrade",
		"--upgrade-owner-id", "owner",
		"--io-drain-timeout", "30s", "--allow-unsafe-upgrade",
		"--status-poll-interval", "1s", "--lock-only",
		"--max-permitted-clock-drift", "1s")
}

func (s *formatSpecificTestSuite) TestRepositoryUpgradeStatusWhileLocked(t *testing.T) {
	t.Parallel()

	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")

	env.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	switch s.formatVersion {
	case format.FormatVersion1:
		require.Contains(t, out, "Format version:      1")
		lockRepositoryForUpgrade(t, env)

		// verify that non-owner clients will fail to connect/upgrade
		env.RunAndExpectFailure(t, "repository", "upgrade",
			"--upgrade-owner-id", "non-owner",
			"--io-drain-timeout", "15s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s", "--upgrade-no-block",
			"--max-permitted-clock-drift", "1s")

		// until we drain, we would be able to see the upgrade status as
		// "Draining"
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block", "--upgrade-owner-id", "owner")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 1 -> 3")
		require.Contains(t, out, "Upgrade Owner:       owner")
		require.Contains(t, out, "Upgrade lock:        Locked")
		require.Contains(t, out, "Lock status:         Draining")

		// attempt to rollback the upgrade and restart
		env.RunAndExpectSuccess(t, "repository", "upgrade", "rollback", "--force", "--upgrade-owner-id", "owner")
		lockRepositoryForUpgrade(t, env)

		// drain all clients
		t.Log("Waiting to drain all clients ...")
		// drain time [30 (io-drain-timeout) * 2 + 1(max clock drift)] + buffer [1 sec]
		time.Sleep(62 * time.Second)

		// verify that access is denied after we drain
		env.RunAndExpectFailure(t, "repository", "status", "--upgrade-no-block")

		// verify that owner clients can check status
		out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-owner-id", "owner")
		require.Contains(t, out, "Ongoing upgrade:     Upgrading from format version 1 -> 3")
		require.Contains(t, out, "Upgrade Owner:       owner")
		require.Contains(t, out, "Upgrade lock:        Locked")
		require.Contains(t, out, "Lock status:         Fully Established")

		// finalize the upgrade
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "15s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")

		// verify that non-owner clients can resume access
		env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	case format.FormatVersion2:
		require.Contains(t, out, "Format version:      2")

		// perform the upgrade
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository has been successfully upgraded.")

		// verify that non-owner clients can resume access
		env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	default:
		require.Contains(t, out, "Format version:      3")
		_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repository", "upgrade",
			"--upgrade-owner-id", "owner",
			"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
			"--status-poll-interval", "1s",
			"--max-permitted-clock-drift", "1s")
		require.Contains(t, stderr, "Repository format is already upto date.")
	}

	out = env.RunAndExpectSuccess(t, "repository", "status", "--upgrade-no-block")
	require.Contains(t, out, "Epoch Manager:       enabled")
	require.Contains(t, out, "Index Format:        v2")
	require.Contains(t, out, "Format version:      3")
}

func TestRepositoryUpgrade_checkIndexInfo(t *testing.T) {
	tcs := []struct {
		indexInfo0   index.Info
		indexInfo1   index.Info
		expectRegexs []string
	}{
		{
			indexInfo0:   &index.InfoStruct{PackBlobID: "a"},
			indexInfo1:   &index.InfoStruct{PackBlobID: "a"},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{PackBlobID: "a"},
			indexInfo1: &index.InfoStruct{PackBlobID: "b"},
			expectRegexs: []string{
				`do not match: "a", "b".*PackBlobID`,
			},
		},
		{
			indexInfo0:   &index.InfoStruct{TimestampSeconds: 1},
			indexInfo1:   &index.InfoStruct{TimestampSeconds: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{TimestampSeconds: 1},
			indexInfo1: &index.InfoStruct{TimestampSeconds: 2},
			expectRegexs: []string{
				"do not match.*TimestampSeconds",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{OriginalLength: 1},
			indexInfo1:   &index.InfoStruct{OriginalLength: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{OriginalLength: 1},
			indexInfo1: &index.InfoStruct{OriginalLength: 2},
			expectRegexs: []string{
				"do not match.*OriginalLength",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{PackedLength: 1},
			indexInfo1:   &index.InfoStruct{PackedLength: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{PackedLength: 1},
			indexInfo1: &index.InfoStruct{PackedLength: 2},
			expectRegexs: []string{
				"do not match.*PackedLength",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{PackOffset: 1},
			indexInfo1:   &index.InfoStruct{PackOffset: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{PackOffset: 1},
			indexInfo1: &index.InfoStruct{PackOffset: 2},
			expectRegexs: []string{
				"do not match.*PackOffset",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{Deleted: true},
			indexInfo1:   &index.InfoStruct{Deleted: true},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{Deleted: false},
			indexInfo1: &index.InfoStruct{Deleted: true},
			expectRegexs: []string{
				"do not match.*Deleted",
			},
		},
		// simple logic error can make result of this false... so check
		{
			indexInfo0:   &index.InfoStruct{Deleted: false},
			indexInfo1:   &index.InfoStruct{Deleted: false},
			expectRegexs: []string{},
		},
		{
			indexInfo0:   &index.InfoStruct{FormatVersion: 1},
			indexInfo1:   &index.InfoStruct{FormatVersion: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{FormatVersion: 1},
			indexInfo1: &index.InfoStruct{FormatVersion: 2},
			expectRegexs: []string{
				"do not match.*FormatVersion",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{CompressionHeaderID: 1},
			indexInfo1:   &index.InfoStruct{CompressionHeaderID: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{CompressionHeaderID: 1},
			indexInfo1: &index.InfoStruct{CompressionHeaderID: 2},
			expectRegexs: []string{
				"do not match.*CompressionHeaderID",
			},
		},
		{
			indexInfo0:   &index.InfoStruct{EncryptionKeyID: 1},
			indexInfo1:   &index.InfoStruct{EncryptionKeyID: 1},
			expectRegexs: []string{},
		},
		{
			indexInfo0: &index.InfoStruct{EncryptionKeyID: 1},
			indexInfo1: &index.InfoStruct{EncryptionKeyID: 2},
			expectRegexs: []string{
				"do not match.*EncryptionKeyID",
			},
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			report := cli.CheckIndexInfo(tc.indexInfo0, tc.indexInfo1)
			require.Equal(t, len(report), len(tc.expectRegexs), "unexpected report length")
			for i := range tc.expectRegexs {
				require.Regexp(t, tc.expectRegexs[i], report[i], "report does not match regular expression")
			}
		})
	}
}
