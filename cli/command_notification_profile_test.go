package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/notification/notifyprofile"
	"github.com/kopia/kopia/notification/sender/nats"
	"github.com/kopia/kopia/notification/sender/webhook"
	"github.com/kopia/kopia/snapshot"
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

func TestNotificationProfile_Nats(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	var profiles []notifyprofile.Summary

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Empty(t, profiles)

	// setup a profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "nats", "--profile-name=mynats", "--nats-server-url=nats://localhost:14222", "--nats-subject=kopia.notifications")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "list", "--json"), &profiles)
	require.Len(t, profiles, 1)
	require.Equal(t, "nats", profiles[0].Type)

	// define another profile
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "nats", "--profile-name=myothernats", "--min-severity=warning",
		"--nats-server-url=nats://anotherhost:4222", "--nats-subject=kopia.other", "--nats-username=alice", "--nats-password=hunter2")

	lines := e.RunAndExpectSuccess(t, "notification", "profile", "list")

	require.Contains(t, lines, "Profile \"mynats\" Type \"nats\" Minimum Severity: report")
	require.Contains(t, lines, "Profile \"myothernats\" Type \"nats\" Minimum Severity: warning")

	var opt notifyprofile.Config

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myothernats", "--json", "--raw"), &opt)

	require.Equal(t, []string{
		"Profile \"myothernats\" Type \"nats\" Minimum Severity: warning",
		"NATS nats://anotherhost:4222 subject \"kopia.other\" format \"txt\"",
	}, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myothernats"))

	var opt2 nats.Options

	require.NoError(t, opt.MethodConfig.Options(&opt2))
	require.Equal(t, "alice", opt2.Username)
	require.Equal(t, "hunter2", opt2.Password)

	// partial update
	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "nats", "--profile-name=myothernats", "--nats-subject=kopia.changed", "--format=html")

	require.Equal(t, []string{
		"Profile \"myothernats\" Type \"nats\" Minimum Severity: warning",
		"NATS nats://anotherhost:4222 subject \"kopia.changed\" format \"html\"",
	}, e.RunAndExpectSuccess(t, "notification", "profile", "show", "--profile-name=myothernats"))

	// delete non-existent profile does not fail
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=unknown")

	// delete existing profiles
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=myothernats")
	e.RunAndExpectSuccess(t, "notification", "profile", "delete", "--profile-name=mynats")

	// no profiles left
	require.Empty(t, e.RunAndExpectSuccess(t, "notification", "profile", "list"))
}

// TestNotificationProfile_SnapshotReportIncludesManifestID is a regression test for a bug where
// the manifest ID embedded in a snapshot-report notification was always blank, because the report
// data was copied from the in-progress manifest before snapshot.SaveSnapshot() assigned its ID.
func TestNotificationProfile_SnapshotReportIncludesManifestID(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "notification", "profile", "configure", "testsender", "--profile-name=mysender", "--format=txt")

	tmplFile := filepath.Join(testutil.TempDirectory(t), "snapshot-report.txt")
	require.NoError(t, os.WriteFile(tmplFile, []byte(
		"Subject: report\n\n{{ range .EventArgs.Snapshots }}id={{ .Manifest.ID }}{{ end }}\n"), 0o600))

	e.RunAndExpectSuccess(t, "notification", "template", "set", "snapshot-report.txt", "--from-file="+tmplFile)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1"), []byte("hello"), 0o600))

	var man snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", srcDir, "--json"), &man)
	require.NotEmpty(t, man.ID)

	var found bool

	for _, m := range e.NotificationsSent() {
		if strings.Contains(m.Body, "id="+string(man.ID)) {
			found = true
			break
		}
	}

	require.True(t, found, "expected a notification body containing the snapshot manifest ID %q", man.ID)
}
