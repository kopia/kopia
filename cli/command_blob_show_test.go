package cli_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestBlobShow(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	someNBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=n")[0], " ")[0]
	someQBlob := strings.Split(env.RunAndExpectSuccess(t, "blob", "list", "--prefix=q")[0], " ")[0]
	env.RunAndExpectSuccess(t, "blob", "show", someNBlob)
	env.RunAndExpectSuccess(t, "blob", "show", someQBlob)
	env.RunAndExpectSuccess(t, "blob", "show", "--decrypt", someNBlob)
	// --decrypt will be ignored
	env.RunAndExpectSuccess(t, "blob", "show", "--decrypt", someQBlob)
}
