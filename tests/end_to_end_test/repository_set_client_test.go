package endtoend_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestRepositorySetClient(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create",
		"filesystem", "--path", e.RepoDir, "--description", "My Repo",
		"--override-username", "myuser",
		"--override-hostname", "myhost",
		"--repository-format-cache-duration=7m")

	sl := e.RunAndExpectSuccess(t, "repo", "status")
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Description:") && strings.Contains(l, "My Repo")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Read-only:") && strings.Contains(l, "false")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Username:") && strings.Contains(l, "myuser")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Hostname:") && strings.Contains(l, "myhost")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Format blob cache:") && strings.Contains(l, "7m0s")
	})

	e.RunAndExpectSuccess(t, "repo", "set-client",
		"--read-only",
		"--description", "My Updated Repo",
		"--hostname", "my-updated-host",
		"--disable-repository-format-cache",
	)

	sl = e.RunAndExpectSuccess(t, "repo", "status")
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Description:") && strings.Contains(l, "My Updated Repo")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Read-only:") && strings.Contains(l, "true")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Hostname:") && strings.Contains(l, "my-updated-host")
	})
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Format blob cache:") && strings.Contains(l, "disabled")
	})

	// repo is read-only
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	// set to read-write and snapshot will now succeeded
	e.RunAndExpectSuccess(t, "repo", "set-client", "--read-write", "--repository-format-cache-duration=5s")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	sl = e.RunAndExpectSuccess(t, "repo", "status")
	verifyHasLine(t, sl, func(l string) bool {
		return strings.Contains(l, "Format blob cache:") && strings.Contains(l, "5s")
	})
}

func verifyHasLine(t *testing.T, lines []string, ok func(s string) bool) {
	t.Helper()

	if slices.ContainsFunc(lines, ok) {
		return
	}

	t.Errorf("output line meeting given condition was not found")
}
