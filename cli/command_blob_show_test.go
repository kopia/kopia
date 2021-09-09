package cli_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestBlobShow(t *testing.T) {
	env := testenv.NewCLITest(t, s.formatFlags, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	var hasEpochManager bool

	for _, line := range env.RunAndExpectSuccess(t, "repository", "status") {
		if strings.HasPrefix(line, "Epoch Manager:") && strings.Contains(line, "enabled") {
			hasEpochManager = true
		}
	}

	someQBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=q")[0], " ")[0]

	if hasEpochManager {
		someXNBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=xn")[0], " ")[0]
		env.RunAndExpectSuccess(t, "blob", "show", someXNBlob)
		env.RunAndExpectSuccess(t, "blob", "show", "--decrypt", someXNBlob)
	} else {
		someNBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=n")[0], " ")[0]
		env.RunAndExpectSuccess(t, "blob", "show", someNBlob)
		env.RunAndExpectSuccess(t, "blob", "show", "--decrypt", someNBlob)
	}

	env.RunAndExpectSuccess(t, "blob", "show", someQBlob)
	// --decrypt will be ignored
	env.RunAndExpectSuccess(t, "blob", "show", "--decrypt", someQBlob)
}
