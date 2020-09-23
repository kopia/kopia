package endtoend_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestReconnect(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "repo", "status")
}

func TestReconnectUsingToken(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	lines := e.RunAndExpectSuccess(t, "repo", "status", "-t", "-s")
	prefix := "$ kopia "

	var reconnectArgs []string

	// look for output line containing the prefix - this will be our reconnect command
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			reconnectArgs = strings.Split(strings.TrimPrefix(l, prefix), " ")
		}
	}

	if reconnectArgs == nil {
		t.Fatalf("can't find reonnect command in kopia repo status output")
	}

	e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, reconnectArgs...)
	e.RunAndExpectSuccess(t, "repo", "status")
}
