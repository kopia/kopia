package cli_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/editor"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/tests/testenv"
)

func TestNotificationTemplates(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	defaultTemplates := e.RunAndExpectSuccess(t, "notification", "template", "list")
	require.Len(t, defaultTemplates[1:], len(notifytemplate.SupportedTemplates()))

	// initially all templates are built-in
	for _, line := range defaultTemplates[1:] {
		require.Contains(t, line, "<built-in>")
	}

	// override 'test-notification.txt' template from STDIN and verify.
	runner.SetNextStdin(strings.NewReader("Subject: test-template-subject\n\ntest-template-body1\ntest-template-body2\n"))
	e.RunAndExpectSuccess(t, "notification", "template", "set", "test-notification.txt", "--from-stdin")

	verifyTemplateContents(t, e, "test-notification.txt", []string{
		"Subject: test-template-subject",
		"",
		"test-template-body1",
		"test-template-body2",
	})

	// make sure it shows up as <customized>
	verifyHasLine(t, e.RunAndExpectSuccess(t, "notification", "template", "list"), func(s string) bool {
		return strings.Contains(s, "test-notification.txt") && strings.Contains(s, "<customized>")
	})

	// reset 'test-notification.txt' template and verify.
	e.RunAndExpectSuccess(t, "notification", "template", "remove", "test-notification.txt")

	// make sure it shows up as <built-in>
	verifyHasLine(t, e.RunAndExpectSuccess(t, "notification", "template", "list"), func(s string) bool {
		return strings.Contains(s, "test-notification.txt") && strings.Contains(s, "<built-in>")
	})

	// now the same using external file
	td := t.TempDir()
	fname := td + "/template.md"

	// no such file
	e.RunAndExpectFailure(t, "notification", "template", "set", "test-notification.txt", "--from-file="+fname)

	os.WriteFile(fname, []byte("Subject: test-template-subject\n\ntest-template-body3\ntest-template-body4\n"), 0o600)
	e.RunAndExpectSuccess(t, "notification", "template", "set", "test-notification.txt", "--from-file="+fname)
	verifyTemplateContents(t, e, "test-notification.txt", []string{
		"Subject: test-template-subject",
		"",
		"test-template-body3",
		"test-template-body4",
	})

	// override editor for the next part
	oldEditor := editor.EditFile
	defer func() { editor.EditFile = oldEditor }()

	invokedSecond := false

	// when editor is first invoked, it will show the old template
	// try setting the template to an invalid value and verify that the editor is invoked again.
	editor.EditFile = func(ctx context.Context, filename string) error {
		b, err := os.ReadFile(filename)
		require.NoError(t, err)

		// verify we got the old version of the template
		require.Equal(t, "Subject: test-template-subject\n\ntest-template-body3\ntest-template-body4\n", string(b))

		// write an invalid template that fails to parse
		os.WriteFile(filename, []byte("Subject: test-template-subject\n\ntest-template-body5 {{\ntest-template-body6\n"), 0o600)

		// editor will be invoked again
		editor.EditFile = func(ctx context.Context, filename string) error {
			invokedSecond = true

			// this time we write the corrected template
			os.WriteFile(filename, []byte("Subject: test-template-subject\n\ntest-template-body5\ntest-template-body6\n"), 0o600)

			return nil
		}
		return nil
	}

	e.RunAndExpectSuccess(t, "notification", "template", "set", "test-notification.txt", "--editor")

	verifyTemplateContents(t, e, "test-notification.txt", []string{
		"Subject: test-template-subject",
		"",
		"test-template-body5",
		"test-template-body6",
	})

	require.True(t, invokedSecond)
}

func verifyTemplateContents(t *testing.T, e *testenv.CLITest, templateName string, expectedLines []string) {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, "notification", "template", "show", ""+templateName)
	require.Equal(t, expectedLines, lines)
}

func verifyHasLine(t *testing.T, lines []string, ok func(s string) bool) {
	t.Helper()

	for _, l := range lines {
		if ok(l) {
			return
		}
	}

	t.Errorf("output line meeting given condition was not found: %v", lines)
}
