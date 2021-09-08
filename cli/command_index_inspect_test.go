package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestIndexInspect(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	someIndex := strings.Split(env.RunAndExpectSuccess(t, "index", "list")[0], " ")[0]
	someContentID := env.RunAndExpectSuccess(t, "content", "list")[0]
	env.RunAndExpectSuccess(t, "index", "inspect", someIndex)
	env.RunAndExpectSuccess(t, "index", "inspect", "--active")
	env.RunAndExpectSuccess(t, "index", "inspect", "--all")

	require.Len(t, env.RunAndExpectSuccess(t, "index", "inspect", "--active", "--content-id", someContentID), 1)
	require.Empty(t, env.RunAndExpectSuccess(t, "index", "inspect", "--active", "--content-id", "nosuchcontent"))

	// now rewrite one content, making it appear in second index
	env.RunAndExpectSuccess(t, "content", "rewrite", someContentID, "--safety=none")
	require.Len(t, env.RunAndExpectSuccess(t, "index", "inspect", "--all", "--content-id", someContentID), 2)

	// no targets specified
	env.RunAndExpectFailure(t, "index", "inspect")
}
