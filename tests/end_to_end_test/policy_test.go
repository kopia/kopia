package endtoend_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
)

func TestDefaultGlobalPolicy(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// make sure we can read policy
	e.RunAndExpectSuccess(t, "policy", "show", "--global")

	// verify we created global policy entry

	var contents []content.Info

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "content", "ls", "--json"), &contents)

	if got, want := len(contents), 1; got != want {
		t.Fatalf("unexpected number of contents %v, want %v", got, want)
	}

	globalPolicyContentID := contents[0].ContentID
	e.RunAndExpectSuccess(t, "content", "show", "-jz", globalPolicyContentID.String())

	// make sure the policy is visible in the manifest list
	var manifests []manifest.EntryMetadata

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "manifest", "list", "--filter=type:policy", "--filter=policyType:global", "--json"), &manifests)

	if got, want := len(manifests), 1; got != want {
		t.Fatalf("unexpected number of manifests %v, want %v", got, want)
	}

	// make sure the policy is visible in the policy list
	var plist []policy.TargetWithPolicy

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "policy", "list", "--json"), &plist)

	if got, want := len(plist), 1; got != want {
		t.Fatalf("unexpected number of policies %v, want %v", got, want)
	}
}

func TestPolicyFollowSymlinks(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	followSymlinksRow := func(args ...string) string {
		t.Helper()

		for _, line := range e.RunAndExpectSuccess(t, append([]string{"policy", "show"}, args...)...) {
			if strings.Contains(line, "Follow symbolic links:") {
				return line
			}
		}

		require.FailNow(t, "policy show output has no follow symbolic links row")

		return ""
	}

	require.Contains(t, followSymlinksRow("--global"), "false")

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--follow-symlinks=true")
	require.Contains(t, followSymlinksRow("--global"), "true")

	e.RunAndExpectSuccess(t, "policy", "set", "/tmp/some-source", "--max-file-size=1000")
	require.Contains(t, followSymlinksRow("/tmp/some-source"), "true")

	e.RunAndExpectSuccess(t, "policy", "set", "/tmp/some-source", "--follow-symlinks=false")
	require.Contains(t, followSymlinksRow("/tmp/some-source"), "false")

	e.RunAndExpectSuccess(t, "policy", "set", "/tmp/some-source", "--follow-symlinks=inherit")
	require.Contains(t, followSymlinksRow("/tmp/some-source"), "true")

	e.RunAndExpectFailure(t, "policy", "set", "--global", "--follow-symlinks=maybe")
}
