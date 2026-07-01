package iomem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIOMemHints exercises the happy path. On Linux it issues real
// FADV_SEQUENTIAL / FADV_DONTNEED syscalls; on other OSes it confirms the
// stubs accept a real *os.File without erroring. No build tag, so it runs
// on every platform.
func TestIOMemHints(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))

	f, err := os.Open(p)
	require.NoError(t, err)

	// require cannot be used inside Cleanup because it calls t.Fatal()/t.FailNow()
	t.Cleanup(func() { assert.NoError(t, f.Close()) })

	require.NoError(t, HintStreaming(f))
	require.NoError(t, HintNotNeeded(f))
}

// TestHintsNilFile verifies the package's nil-input contract: both Hint
// helpers must error on a nil file on every OS (Linux via callWithFd's
// nil-guard; non-Linux via the stub nil-checks).
func TestHintsNilFile(t *testing.T) {
	require.Error(t, HintStreaming(nil))
	require.Error(t, HintNotNeeded(nil))
}
