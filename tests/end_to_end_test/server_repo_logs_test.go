package endtoend_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
)

// Verify that the "diagnostic/log" blobs are uploaded to the  repository when
// the server exits.
// Approach / steps:
// - initialize a repo, note this uploads logs to the repo
// - start the server
// - create a "snapshot source" on the server via the server-control API
// - remove all log blobs from the repo and check that there are 0.
// - stop the server
// - check whether or not the server uploaded the logs.
func TestServerRepoLogsUploadedOnShutdown(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	logs := e.RunAndExpectSuccess(t, "logs", "list")
	require.Len(t, logs, 1, "repo create did not upload logs")

	var sp testutil.ServerParameters

	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--insecure",
		"--without-password",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation,
	)

	require.NotEmpty(t, sp.BaseURL, "server base URL")

	controlCli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:  sp.BaseURL,
		Username: defaultServerControlUsername,
		Password: sp.ServerControlPassword,
	})
	require.NoError(t, err)

	checkServerStartedOrFailed := func() bool {
		var hs apiclient.HTTPStatusError

		_, err := serverapi.Status(ctx, controlCli)

		if errors.As(err, &hs) {
			switch hs.HTTPStatusCode {
			case http.StatusBadRequest:
				return false
			case http.StatusForbidden:
				return false
			}
		}

		return true
	}

	require.Eventually(t, checkServerStartedOrFailed, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, controlCli.FetchCSRFTokenForTesting(ctx))

	keepDaily := policy.OptionalInt(3)

	_, err = serverapi.CreateSnapshotSource(ctx, controlCli, &serverapi.CreateSnapshotSourceRequest{
		Path: sharedTestDataDir1,
		Policy: &policy.Policy{
			RetentionPolicy: policy.RetentionPolicy{
				KeepDaily: &keepDaily,
			},
		},
		CreateSnapshot: false,
	})

	require.NoError(t, err)

	lines := e.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	t.Logf("lines: %v", lines)

	e.RunAndExpectSuccess(t, "logs", "cleanup", "--max-age=1ns")
	logs = e.RunAndExpectSuccess(t, "logs", "list")
	require.Empty(t, logs, "new logs were uploaded unexpectedly:", logs)

	require.NoError(t, serverapi.Shutdown(ctx, controlCli))
	require.NoError(t, wait())

	logs = e.RunAndExpectSuccess(t, "logs", "list")

	require.NotEmpty(t, logs, "server logs were not uploaded")
}
