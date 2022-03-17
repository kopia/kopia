package kopiarunner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpgradeFormatVersion(t *testing.T) {
	repoDir := t.TempDir()

	ks, err := NewKopiaSnapshotter(repoDir)
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
