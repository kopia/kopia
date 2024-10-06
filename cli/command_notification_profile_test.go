package cli_test

import (
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

	var profiles []notifyprofile.Summary

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Empty(t, profiles)

	// setup a profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "testsender", "--profile-name=mywebhook")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Len(t, profiles, 1)
	require.Equal(t, "testsender", profiles[0].Type)

	// nothing is sent so far
	require.Empty(t, e.NotificationsSent())

	// now send a test message
	e.RunAndExpectSuccess(t, "notification", "profile", "test", "--profile-name=mywebhook")

	// make sure we received the test request
	require.Len(t, e.NotificationsSent(), 1)
	require.Contains(t, e.NotificationsSent()[0].Body, "If you received this, your notification configuration")

	// define another profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "testsender", "--profile-name=myotherwebhook", "--min-severity=warning")

	lines := e.RunAndExpectSuccess(t, "notification", "profile", "list")

	require.Contains(t, lines, "Profile \"mywebhook\" Type \"testsender\" Minimum Severity: report")
	require.Contains(t, lines, "Profile \"myotherwebhook\" Type \"testsender\" Minimum Severity: warning")

	// delete non-existent profile does not fail
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=unknown")

	// delete existing profiles
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=myotherwebhook")
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=mywebhook")

	// no profiles left
	require.Empty(t, e.RunAndExpectSuccess(t, "notification", "profile", "list"))
}
