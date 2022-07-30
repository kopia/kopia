package kopiarunner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/format"
)

func TestUpgradeFormatVersion(t *testing.T) {
	repoDir := t.TempDir()

	ks, err := NewKopiaSnapshotter(repoDir)
	if errors.Is(err, ErrExeVariableNotSet) {
		t.Skip("KOPIA_EXE not set, skipping test")
	}
	// Create repo in an old version
	err = ks.CreateRepo("filesystem", "--path="+repoDir, "--format-version", "1")
	require.NoError(t, err)

	rs, err := ks.GetRepositoryStatus()
	require.NoError(t, err)

	prev := rs.ContentFormat.MutableParameters.Version
	require.Equal(t, prev, format.Version(1), "The format version should be 1.")

	ks.UpgradeRepository(repoDir)

	rs, err = ks.GetRepositoryStatus()
	require.NoError(t, err)

	got := rs.ContentFormat.MutableParameters.Version
	require.Equal(t, got, format.Version(2), "The format version should be upgraded to 2.")

	require.NotEqual(t, got, prev, "The format versions should be different.")
}
