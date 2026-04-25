package localfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

func TestOpenWithContext_Success(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	fn := filepath.Join(tmp, "testfile")
	require.NoError(t, os.WriteFile(fn, []byte("hello"), 0o644))

	f, err := openWithContext(context.Background(), fn)
	require.NoError(t, err)
	require.NotNil(t, f)
	f.Close()
}

func TestOpenWithContext_FileNotFound(t *testing.T) {
	f, err := openWithContext(context.Background(), "/no/such/file")
	require.Error(t, err)
	require.Nil(t, f)
	require.True(t, os.IsNotExist(err))
}

func TestOpenWithContext_CancelledContext(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	fn := filepath.Join(tmp, "testfile")
	require.NoError(t, os.WriteFile(fn, []byte("hello"), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// With an already-cancelled context, os.Open may complete before the
	// context check runs. Either outcome (success or context.Canceled) is
	// acceptable — the key property is that it does not block.
	f, err := openWithContext(ctx, fn)
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	} else {
		f.Close()
	}
}

// TestOpenWithContext_TimeoutOnNonBlockingFile verifies that a normal file
// opens promptly when there's a 5s deadline — the deadline should never fire.
// This guards against a regression where openWithContext mistakenly enforces
// the timeout even on fast paths.
func TestOpenWithContext_TimeoutOnNonBlockingFile(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	fn := filepath.Join(tmp, "testfile")
	require.NoError(t, os.WriteFile(fn, []byte("hello"), 0o644))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f, err := openWithContext(ctx, fn)
	require.NoError(t, err)
	require.NotNil(t, f)
	f.Close()
}

func TestOpenWithContext_Directory(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	f, err := openWithContext(context.Background(), tmp)
	require.NoError(t, err)
	require.NotNil(t, f)
	f.Close()
}
