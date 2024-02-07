package os_snapshot_test

import (
	"os"
	"testing"

	"github.com/mxk/go-vss"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/tempfile"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestShadowCopy(t *testing.T) {
	kopiaExe := os.Getenv("KOPIA_EXE")
	if kopiaExe == "" {
		t.Skip()
	}

	runner := testenv.NewExeRunnerWithBinary(t, kopiaExe)

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	root := testutil.TempDirectory(t)
	f, err := tempfile.Create(root)
	require.NoError(t, err)
	_, err = f.WriteString("locked file\n")
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	defer f.Close()

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--enable-volume-shadow-copy=when-available")

	_, err = vss.Get("{00000000-0000-0000-0000-000000000000}")

	isAdmin := !errors.Is(err, os.ErrPermission)
	if isAdmin {
		t.Log("Running as admin, expecting snapshot creation to succeed")
		e.RunAndExpectSuccess(t, "snap", "create", root)
	} else {
		t.Log("Not running as admin, expecting snapshot creation to fail")
		e.RunAndExpectFailure(t, "snap", "create", root)
	}

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	require.NotEmpty(t, sources)
	require.NotEmpty(t, sources[0].Snapshots)

	oid := sources[0].Snapshots[0].ObjectID
	entries := clitestutil.ListDirectory(t, e, oid)

	if isAdmin {
		lines := e.RunAndExpectSuccess(t, "show", entries[0].ObjectID)
		require.Equal(t, []string{"locked file"}, lines)
	} else {
		require.Empty(t, entries)
	}
}
