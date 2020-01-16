package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestDefaultGlobalPolicy(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// make sure we can read policy
	e.RunAndExpectSuccess(t, "policy", "show", "--global")

	// verify we created global policy entry
	globalPolicyBlockID := e.RunAndVerifyOutputLineCount(t, 1, "content", "ls")[0]
	e.RunAndExpectSuccess(t, "content", "show", "-jz", globalPolicyBlockID)

	// make sure the policy is visible in the manifest list
	e.RunAndVerifyOutputLineCount(t, 1, "manifest", "list", "--filter=type:policy", "--filter=policyType:global")

	// make sure the policy is visible in the policy list
	e.RunAndVerifyOutputLineCount(t, 1, "policy", "list")
}
