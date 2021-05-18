package cli_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestIndexInspect(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	someIndex := strings.Split(env.RunAndExpectSuccess(t, "index", "list")[0], " ")[0]
	env.RunAndExpectSuccess(t, "index", "inspect", someIndex)
}
