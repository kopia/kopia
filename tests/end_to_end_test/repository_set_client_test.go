package endtoend_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositorySetClient(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--description", "My Repo", "--override-username", "myuser", "--override-hostname", "myhost")

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

	e.RunAndExpectSuccess(t, "repo", "set-client",
		"--read-only",
		"--description", "My Updated Repo",
		"--hostname", "my-updated-host")

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

	// repo is read-only
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1)

	// set to read-write and snapshot will now succeeded
	e.RunAndExpectSuccess(t, "repo", "set-client", "--read-write")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
}

func verifyHasLine(t *testing.T, lines []string, ok func(s string) bool) {
	t.Helper()

	for _, l := range lines {
		if ok(l) {
			return
		}
	}

	t.Errorf("output line meeting given condition was not found")
}
