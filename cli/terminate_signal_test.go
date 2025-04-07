package cli_test

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestTerminate(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	var sp testutil.ServerParameters

	wait, interrupt := env.RunAndProcessStderrInt(t, sp.ProcessOutput, nil, "server", "start",
		"--address=localhost:0",
		"--insecure")

	interrupt(syscall.SIGTERM)

	require.NoError(t, wait())
}
