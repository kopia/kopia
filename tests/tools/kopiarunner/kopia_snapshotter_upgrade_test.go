package kopiarunner

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpgradeFormatVersion(t *testing.T) {
	baseDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(baseDir)

	repoDir, err := os.MkdirTemp(baseDir, "repo")
	require.NoError(t, err)

	_, err = os.MkdirTemp(baseDir, "source")
	require.NoError(t, err)

	ks, err := NewKopiaSnapshotter(repoDir)
	if errors.Is(err, ErrExeVariableNotSet) {
		t.Skip("KOPIA_EXE not set, skipping test")
	}

	require.NoError(t, err)

	// Create repo in an old version
	err = ks.CreateRepo("filesystem", "--path="+repoDir, "--format-version", "1")
	require.NoError(t, err)

	prev := ks.GetRepositoryStatus("Format version")
	require.Equal(t, prev, "1", "The format version should be 1.")

	ks.UpgradeRepository(repoDir)

	got := ks.GetRepositoryStatus("Format version")

	require.Equal(t, got, "2", "The format version should be upgraded to 2.")

	require.NotEqual(t, got, prev, "The format versions should be different.")
}
