package endtoend_test

import (
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/tests/testenv"
)

// Pattern in stderr that `kopia server` uses to pass ephemeral data.
const (
	serverOutputAddress    = "SERVER ADDRESS: "
	serverOutputCertSHA256 = "SERVER CERT SHA256: "
	serverOutputPassword   = "SERVER PASSWORD: "
)

type serverParameters struct {
	baseURL           string
	sha256Fingerprint string
	password          string
}

func (s *serverParameters) ProcessOutput(l string) bool {
	if strings.HasPrefix(l, serverOutputAddress) {
		s.baseURL = strings.TrimPrefix(l, serverOutputAddress)
		return false
	}

	if strings.HasPrefix(l, serverOutputCertSHA256) {
		s.sha256Fingerprint = strings.TrimPrefix(l, serverOutputCertSHA256)
	}

	if strings.HasPrefix(l, serverOutputPassword) {
		s.password = strings.TrimPrefix(l, serverOutputPassword)
	}

	return true
}

func TestServerStart(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Start the server and wait for it to auto-shutdown in 3 seconds.
	// If this does not work, we avoid starting a longer test.
	e.RunAndExpectSuccess(t, "server", "start", "--ui", "--auto-shutdown=3s")
	time.Sleep(3 * time.Second)

	var sp serverParameters

	e.RunAndProcessStderr(t, sp.ProcessOutput, "server", "start", "--ui", "--random-password", "--tls-generate-cert", "--auto-shutdown=60s")
	t.Logf("detected server parameters %#v", sp)

	cli, err := serverapi.NewClient(serverapi.ClientOptions{
		BaseURL:                             sp.baseURL,
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
	})
	if err != nil {
		t.Fatalf("unable to create API client")
	}

	time.Sleep(1 * time.Second)

	if err := cli.Get("status", &serverapi.Empty{}); err != nil {
		t.Errorf("status error: %v", err)
	}

	// TODO - add more tests

	// explicit shutdown
	if err := cli.Post("shutdown", &serverapi.Empty{}, &serverapi.Empty{}); err != nil {
		t.Logf("expected shutdown error: %v", err)
	}
}
