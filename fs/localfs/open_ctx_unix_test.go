//go:build !windows

package localfs

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
)

// TestOpenWithContext_TimeoutFiresOnBlockingOpen exercises the actual timeout
// path: a posix FIFO blocks on open until the other end opens, so a context
// deadline must cause openWithContext to return promptly with DeadlineExceeded.
// FIFOs are unix-only; the equivalent Windows scenario (a locked file held
// open by another process with restrictive sharing modes) requires
// platform-specific setup and is intentionally not exercised here.
func TestOpenWithContext_TimeoutFiresOnBlockingOpen(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	fifoPath := filepath.Join(tmp, "blocker")
	require.NoError(t, syscall.Mkfifo(fifoPath, 0o600))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	f, err := openWithContext(ctx, fifoPath)
	elapsed := time.Since(start)

	require.Nil(t, f)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Timeout was 100ms; the call should return within ~500ms even on slow CI.
	// Without the cancellation path, this would block indefinitely.
	require.Less(t, elapsed, 500*time.Millisecond,
		"openWithContext should honor the context deadline, not wait for the FIFO peer")
}
