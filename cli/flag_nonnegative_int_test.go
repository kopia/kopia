package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

// TestNegativeParallelRejected verifies that a negative --parallel value is
// rejected at flag-parse time (returning an error identifying the bad value)
// rather than panicking once the command runs. See kopia/kopia#2022.
//
// The assertion checks the rejection message specifically, so the test fails if
// the flag ever stops validating: a bare "command failed" check would pass even
// without the fix, since these commands also fail later when no repository is
// connected.
func TestNegativeParallelRejected(t *testing.T) {
	t.Parallel()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	for _, args := range [][]string{
		{"snapshot", "migrate", "--parallel=-1", "--all"},
		{"content", "verify", "--parallel=-1"},
	} {
		_, _, err := env.Run(t, true, args...)
		require.ErrorContains(t, err, "must not be negative",
			"'kopia %v' should fail with the negative-value rejection", strings.Join(args, " "))
	}
}
