//go:build !no_extra_providers

package rclone

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

func TestCreateSupportFilesDirAvoidsOSTempDir(t *testing.T) {
	t.Parallel()

	td, err := createSupportFilesDir()
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(td) //nolint:errcheck
	})

	require.DirExists(t, td)

	if _, err := os.UserCacheDir(); err == nil {
		require.NotEqual(t, filepath.Clean(os.TempDir()), filepath.Dir(filepath.Clean(td)),
			"rclone support files must not live in the OS temp directory - temp cleaners delete them from under a running rclone (#4791)")
	}
}

func TestRCloneStorageSupportFilesDeleted(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	rcloneExe := os.Getenv("RCLONE_EXE")
	if rcloneExe == "" {
		rcloneExe = "rclone"
	}

	if err := exec.CommandContext(ctx, rcloneExe, "version").Run(); err != nil {
		if os.Getenv("CI") == "" {
			t.Skipf("rclone not installed: %v", err)
		} else {
			// on CI fail hard
			t.Fatalf("rclone not installed: %v", err)
		}
	}

	st, err := New(ctx, &Options{
		// pass local file as remote path.
		RemotePath: testutil.TempDirectory(t),
		RCloneExe:  rcloneExe,
	}, true)
	require.NoError(t, err, "unable to connect to rclone backend")

	t.Cleanup(func() {
		st.Close(testlogging.ContextForCleanup(t))
	})

	r, ok := st.(*rcloneStorage)
	require.True(t, ok)

	// simulate a temp cleaner (e.g. Windows Storage Sense) deleting support files
	// from under the running rclone (#4791). rclone only opens the htpasswd file
	// while serving an authenticated request, so right after startup nothing holds
	// it and the deletion succeeds - just like a cleaner sweeping an idle instance.
	require.NoError(t, os.Remove(filepath.Join(r.temporaryDir, "htpasswd")))

	// once the htpasswd file is gone, rclone drops every connection that carries
	// an Authorization header; operations must fail with a clear error instead of
	// appearing to hang.
	var tmp gather.WriteBuffer
	defer tmp.Close()

	t0 := clock.Now()
	err = st.GetBlob(ctx, "someblob1234567812345678", 0, -1, &tmp)
	dur := clock.Now().Sub(t0)

	require.Error(t, err)
	require.ErrorContains(t, err, "error flushing dir cache")
	require.Less(t, dur, time.Minute, "operation against auth-broken rclone took too long: %v", dur)
}
