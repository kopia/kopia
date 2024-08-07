package cli_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/tests/testenv"
)

func TestNotificationProfile(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// setup a temporary web server we will be using as a notification profile
	mux := http.NewServeMux()

	var requestBodies []bytes.Buffer

	mux.HandleFunc("/some-path", func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		io.Copy(&b, r.Body)

		requestBodies = append(requestBodies, b)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	var profiles []notifyprofile.Summary

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Empty(t, profiles)

	// setup a profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile=mywebhook", "--endpoint", server.URL+"/some-path")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Len(t, profiles, 1)
	require.Equal(t, "webhook", profiles[0].Type)

	// nothing is sent so far
	require.Empty(t, requestBodies)

	// now send a test message
	e.RunAndExpectSuccess(t, "notification", "profile", "test", "--profile=mywebhook")

	// make sure we received the test request
	require.Len(t, requestBodies, 1)
	require.Contains(t, requestBodies[0].String(), "If you received this, your notification configuration")

	// define another profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile=myotherwebhook", "--endpoint", server.URL+"/some-other-path", "--min-severity=warning")

	lines := e.RunAndExpectSuccess(t, "notification", "profile", "list")

	require.Contains(t, lines, "Profile \"mywebhook\" Type \"webhook\" Minimum Severity: report")
	require.Contains(t, lines, "Profile \"myotherwebhook\" Type \"webhook\" Minimum Severity: warning")

	// delete non-existent profile does not fail
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile=unknown")

	// delete existing profiles
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile=myotherwebhook")
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile=mywebhook")

	// no profiles left
	require.Empty(t, e.RunAndExpectSuccess(t, "notification", "profile", "list"))
}
