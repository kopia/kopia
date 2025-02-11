package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender/webhook"
	"github.com/kopia/kopia/tests/testenv"
)

func TestNotificationProfile(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// no profiles
	e.RunAndExpectFailure(t, "notification", "profile", "show", "--profile-name=no-such-profile")

	var profiles []notifyprofile.Summary

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Empty(t, profiles)

	// setup a profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "testsender", "--profile-name=mywebhook", "--send-test-notification")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Len(t, profiles, 1)
	require.Equal(t, "testsender", profiles[0].Type)

	// one test message sent
	require.Len(t, e.NotificationsSent(), 1)

	// now send a test message
	e.RunAndExpectSuccess(t, "notification", "profile", "test", "--profile-name=mywebhook")
	e.RunAndExpectFailure(t, "notification", "profile", "show", "--profile-name=no-such-profile")

	// make sure we received the test message
	require.Len(t, e.NotificationsSent(), 2)
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

func TestNotificationProfile_WebHook(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	var profiles []notifyprofile.Summary

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Empty(t, profiles)

	// setup a profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile-name=mywebhook", "--endpoint=http://localhost:12345")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Len(t, profiles, 1)
	require.Equal(t, "webhook", profiles[0].Type)

	// define another profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile-name=myotherwebhook", "--min-severity=warning", "--endpoint=http://anotherhost:12345", "--http-header", "Foo:Bar", "--http-header", "Baz:Qux")

	lines := e.RunAndExpectSuccess(t, "notification", "profile", "list")

	require.Contains(t, lines, "Profile \"mywebhook\" Type \"webhook\" Minimum Severity: report")
	require.Contains(t, lines, "Profile \"myotherwebhook\" Type \"webhook\" Minimum Severity: warning")

	var opt notifyprofile.Config

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myotherwebhook", "--json", "--raw"), &opt)

	var summ notifyprofile.Summary
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myotherwebhook", "--json"), &summ)

	require.Equal(t, []string{
		"Profile \"myotherwebhook\" Type \"webhook\" Minimum Severity: warning",
		"Webhook POST http://anotherhost:12345 Format \"txt\"",
	}, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myotherwebhook"))

	var opt2 webhook.Options

	require.NoError(t, opt.MethodConfig.Options(&opt2))
	require.Equal(t, "Foo:Bar\nBaz:Qux", opt2.Headers)

	// partial update
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "webhook", "--profile-name=myotherwebhook", "--method=PUT", "--format=html")

	require.Equal(t, []string{
		"Profile \"myotherwebhook\" Type \"webhook\" Minimum Severity: warning",
		"Webhook PUT http://anotherhost:12345 Format \"html\"",
	}, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myotherwebhook"))

	// delete non-existent profile does not fail
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=unknown")

	// delete existing profiles
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=myotherwebhook")
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=mywebhook")

	// no profiles left
	require.Empty(t, e.RunAndExpectSuccess(t, "notification", "profile", "list"))
}
