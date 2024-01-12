package cli_test

import (
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

// Waits until the server advertises its address on the line.
func serverStarted(line string) bool {
	return !strings.HasPrefix(line, "SERVER ADDRESS: ")
}

func TestTerminate(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	wait, interrupt := env.RunAndProcessStderrInt(t, serverStarted, "server", "start",
		"--address=localhost:0",
		"--insecure")

	interrupt(syscall.SIGTERM)

	require.NoError(t, wait())
}
