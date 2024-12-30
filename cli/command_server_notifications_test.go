package cli_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerNotifications(t *testing.T) {
	mux := http.NewServeMux()

	notificationsReceived := make(chan string, 100)

	mux.HandleFunc("/notification-webhook", func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		io.Copy(&b, r.Body)

		notificationsReceived <- b.String()
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir0 := testutil.TempDirectory(t)
	dir1 := testutil.TempDirectory(t)
	dir2 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")

	env.RunAndExpectSuccess(t, "snap", "create", dir0)

	env.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env.RepoDir, "--override-username=test-user", "--override-hostname=test-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir1)
	env.RunAndExpectSuccess(t, "snap", "create", dir2)

	// setup webhook notification
	env.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile-name=mywebhook", "--endpoint="+server.URL+"/notification-webhook", "--method=POST", "--format=html")

	var sp testutil.ServerParameters

	env.SetLogOutput(true, "server")

	wait, kill := env.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--insecure",
		"--random-server-control-password",
		"--shutdown-grace-period", "100ms",
	)

	// trigger server snapshot
	env.RunAndExpectSuccess(t, "server", "snapshot", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, dir1)

	t.Logf("triggered")

	select {
	case not := <-notificationsReceived:
		t.Logf("notification received: %v", not)
		assert.Contains(t, not, "snapshotstatus-success")
	case <-time.After(5 * time.Second):
		t.Error("notification not received in time")
	}

	kill()
	wait()
}
