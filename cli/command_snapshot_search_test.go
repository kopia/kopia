package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

type searchJSONMatch struct {
	SnapshotID string              `json:"snapshotID"`
	Source     snapshot.SourceInfo `json:"source"`
	StartTime  fs.UTCTimestamp     `json:"startTime"`
	Path       string              `json:"path"`
	Type       string              `json:"type"`
	ObjectID   string              `json:"objectID"`
	Size       int64               `json:"size"`
}

func TestSnapshotSearch(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	e.Environment["KOPIA_CACHE_DIRECTORY"] = testutil.TempDirectory(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	src := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(filepath.Join(src, "Reports"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "Reports", "Needle.txt"), []byte("older"), 0o600))

	var first, second cli.SnapshotManifest
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", src, "--json"), &first)
	require.NoError(t, os.Remove(filepath.Join(src, "Reports", "Needle.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(src, "Reports", "needle.csv"), []byte("newer"), 0o600))
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", src, "--json"), &second)

	var matches []searchJSONMatch
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "needle", "--ignore-case", "--json"), &matches)
	require.Len(t, matches, 2)
	require.Equal(t, string(second.ID), matches[0].SnapshotID)
	require.Equal(t, "Reports/needle.csv", matches[0].Path)
	require.Equal(t, second.Source, matches[0].Source)
	require.Equal(t, second.StartTime, matches[0].StartTime)
	require.EqualValues(t, 5, matches[0].Size)
	require.Equal(t, string(first.ID), matches[1].SnapshotID)
	require.Equal(t, "Reports/Needle.txt", matches[1].Path)

	for _, match := range matches {
		require.Equal(t, "file", match.Type)
		require.NotEmpty(t, match.ObjectID)
	}

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "needle", "--ignore-case", "--max-results=1", "--json"), &matches)
	require.Len(t, matches, 1)
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "Needle", "--snapshot", string(first.ID), "--json"), &matches)
	require.Len(t, matches, 1)
	require.Equal(t, string(first.ID), matches[0].SnapshotID)
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "Needle", "--snapshot", string(first.ID), "--snapshot", string(first.ID), "--json"), &matches)
	require.Len(t, matches, 1)
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "absent", "--json"), &matches)
	require.NotNil(t, matches)
	require.Empty(t, matches)
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "Reports", "--json"), &matches)
	require.Len(t, matches, 2)
	require.Equal(t, "directory", matches[0].Type)
	lines := e.RunAndExpectSuccess(t, "snapshot", "search", "Needle")
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "Reports/Needle.txt")
	e.RunAndExpectFailure(t, "snapshot", "search", "Needle", "--max-results=-1")
	e.RunAndExpectFailure(t, "snapshot", "search", "Needle", "--snapshot=not-a-snapshot")
	e.RunAndExpectFailure(t, "snapshot", "search", "", "--json")
}

func TestSnapshotSearchAcrossHosts(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	e.Environment["KOPIA_CACHE_DIRECTORY"] = testutil.TempDirectory(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	src := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(src, "needle.txt"), []byte("shared contents"), 0o600))

	var first, otherHost cli.SnapshotManifest
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", src, "--json"), &first)
	e.RunAndExpectSuccess(t, "repo", "set-client", "--hostname=another-host", "--username=another-user")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", src, "--json"), &otherHost)
	e.RunAndExpectSuccess(t, "repo", "set-client", "--hostname", first.Source.Host, "--username", first.Source.UserName)

	var matches []searchJSONMatch
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "search", "needle", "--json"), &matches)
	require.Len(t, matches, 2)
	require.Equal(t, "another-host", matches[0].Source.Host)
	require.Equal(t, "another-user", matches[0].Source.UserName)
	require.Equal(t, string(otherHost.ID), matches[0].SnapshotID)
	require.Equal(t, string(first.ID), matches[1].SnapshotID)
	require.Equal(t, matches[0].ObjectID, matches[1].ObjectID)
}
