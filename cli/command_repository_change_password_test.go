package cli_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositoryChangePassword(t *testing.T) {
	r1 := testenv.NewInProcRunner(t)
	r2 := testenv.NewInProcRunner(t)
	env1 := testenv.NewCLITest(t, r1)
	env2 := testenv.NewCLITest(t, r2)

	env1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env1.RepoDir, "--disable-repository-format-cache")
	env1.RunAndExpectSuccess(t, "snapshot", "ls")

	// connect to repo with --disable-repository-format-cache so that format blob is not cached
	// this makes password changes immediate
	env2.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env1.RepoDir, "--disable-repository-format-cache")
	env2.RunAndExpectSuccess(t, "snapshot", "ls")

	env1.RunAndExpectSuccess(t, "repo", "change-password", "--new-password", "newPass")

	// at this point env2 stops working
	env2.RunAndExpectFailure(t, "snapshot", "ls")

	r3 := testenv.NewInProcRunner(t)

	// new connections will fail when using old (default) password
	env3 := testenv.NewCLITest(t, r3)
	env3.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", env1.RepoDir, "--disable-repository-format-cache")

	// new connections will succeed when using new password
	r3.RepoPassword = "newPass"

	env3.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env1.RepoDir, "--disable-repository-format-cache")
}

func TestRepositoryChangePassword_LegacDisallowed(t *testing.T) {
	env1 := testenv.NewCLITest(t, testenv.NewInProcRunner(t))
	// pass --no-enable-password-change to create repository using old format that does
	// not support password change.
	env1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env1.RepoDir, "--disable-repository-format-cache", "--format-version=0.8")
	env1.RunAndExpectFailure(t, "repo", "change-password", "--new-password", "newPass")
}
