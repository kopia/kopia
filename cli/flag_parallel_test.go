package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

// TestNegativeParallelRejected verifies that a negative --parallel value is
// rejected at flag-parse time and returns an error identifying the bad value
// rather than panicking once the command runs.
func TestNegativeParallelRejected(t *testing.T) {
	t.Parallel()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	for _, args := range [][]string{
		{"snapshot", "migrate", "--parallel=-1", "--all"},
		{"content", "verify", "--parallel=-1"},
	} {
		_, _, err := env.Run(t, true, args...)
		// check rejection message so the test fails if
		// the flag ever stops validating, otherwise a
		// bare "command failed" check would pass
		// since these commands fail regardless given that
		// no repository is connected.
		require.ErrorContains(t, err, "invalid syntax",
			"'kopia %v' should fail parsing the negative value", strings.Join(args, " "))
	}
}
