package endtoend_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/testenv"
)

func TestProfileFlags(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewExeRunner(t))

	// contents not needed on test failure
	diagsDir := t.TempDir()

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "repo", "status",
		"--diagnostics-output-directory", diagsDir,
		"--profile-store-on-exit",
		"--profile-cpu",
		"--profile-blocking-rate=1",
		"--profile-mutex-fraction=1",
		"--profile-memory-rate=1",
	)

	// get per-execution directory
	entries, err := os.ReadDir(diagsDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	pprofDir := filepath.Join(diagsDir, entries[0].Name(), "profiles")

	for _, name := range []string{"cpu.pprof", "allocs.pprof", "block.pprof", "goroutine.pprof", "mutex.pprof", "heap.pprof", "threadcreate.pprof"} {
		f := filepath.Join(pprofDir, name)
		t.Run(f, func(t *testing.T) {
			require.FileExists(t, f, "expected profile file")

			info, err := os.Stat(f)

			require.NoError(t, err)
			require.NotZero(t, info.Size(), "profile file %s should not be empty", f)
		})
	}
}
