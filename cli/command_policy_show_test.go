package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func TestShowPolicyRunMissedWithCron(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--snapshot-time-crontab=0 12 * * *", "--run-missed=true")

	lines := e.RunAndExpectSuccess(t, "policy", "show", "--global")
	require.Contains(t, compressSpaces(lines), " Run missed snapshots: true (defined for this target)")
}
