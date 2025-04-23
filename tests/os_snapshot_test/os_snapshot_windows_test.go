package os_snapshot_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/mxk/go-vss"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

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
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--enable-volume-shadow-copy=when-available")

	// create a file that cannot be accessed and then attempt to create a snapshot
	root := testutil.TempDirectory(t)
	f := createAutoDelete(t, root)
	_, err := f.WriteString("locked file\n")

	require.NoError(t, err)
	require.NoError(t, f.Sync())

	_, err = vss.Get("{00000000-0000-0000-0000-000000000000}")

	isAdmin := !errors.Is(err, os.ErrPermission)
	if isAdmin {
		t.Log("Running as admin, expecting snapshot creation to succeed")
		e.RunAndExpectSuccess(t, "snap", "create", root)
	} else {
		t.Log("Not running as admin, expecting snapshot creation to fail because it cannot access the file that is in use")
		e.RunAndExpectFailure(t, "snap", "create", root)
	}

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	require.NotEmpty(t, sources)
	require.NotEmpty(t, sources[0].Snapshots)

	oid := sources[0].Snapshots[0].ObjectID
	entries := clitestutil.ListDirectory(t, e, oid)
	t.Log("sources[0].Snapshots[0].ObjectID entries:", entries)

	if isAdmin {
		require.NotEmpty(t, entries)
		lines := e.RunAndExpectSuccess(t, "show", entries[0].ObjectID)
		require.Equal(t, []string{"locked file"}, lines)
	} else {
		require.Empty(t, entries)
	}
}

func createAutoDelete(t *testing.T, dir string) *os.File {
	t.Helper()

	fullpath := filepath.Join(dir, uuid.NewString())

	fname, err := syscall.UTF16PtrFromString(fullpath)
	require.NoError(t, err, "constructing file name UTF16Ptr")

	// This call creates a file that's automatically deleted on close.
	h, err := syscall.CreateFile(
		fname,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		uint32(windows.FILE_FLAG_DELETE_ON_CLOSE),
		0)

	require.NoError(t, err, "creating file")

	f := os.NewFile(uintptr(h), fullpath)

	t.Cleanup(func() {
		f.Close()
	})

	return f
}
