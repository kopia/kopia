package cli_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

// TestNegativeParallelRejected verifies that a negative --parallel value is
// rejected at flag-parse time (returning an error) rather than panicking once
// the command runs. See kopia/kopia#2022.
func TestNegativeParallelRejected(t *testing.T) {
	t.Parallel()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	env.RunAndExpectFailure(t, "snapshot", "migrate", "--parallel=-1", "--all")
	env.RunAndExpectFailure(t, "content", "verify", "--parallel=-1")
}
