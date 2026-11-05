package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSetActionsPolicyRemove(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	td := testutil.TempDirectory(t)

	cases := []struct {
		flag  string
		row   string
		value string
	}{
		{"--before-folder-action", "Run command before this folder:", "none"},
		{"--after-folder-action", "Run command after this folder:", "none"},
		{"--before-snapshot-root-action", "Run command before snapshot root:", "none"},
		{"--after-snapshot-root-action", "Run command after snapshot root:", "none"},
		{"--before-snapshot-root-action", "Run command before snapshot root:", "inherit"},
		{"--after-snapshot-root-action", "Run command after snapshot root:", "inherit"},
		{"--before-folder-action", "Run command before this folder:", ""},
	}

	for _, tc := range cases {
		e.RunAndExpectSuccess(t, "policy", "set", td, tc.flag, "some-command")
		require.Contains(t, strings.Join(e.RunAndExpectSuccess(t, "policy", "show", td), "\n"), tc.row)

		e.RunAndExpectSuccess(t, "policy", "set", td, tc.flag+"="+tc.value)
		require.NotContains(t, strings.Join(e.RunAndExpectSuccess(t, "policy", "show", td), "\n"), tc.row, "%v=%q", tc.flag, tc.value)
	}
}
