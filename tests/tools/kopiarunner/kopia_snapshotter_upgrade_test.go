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

	// Create repo in sourceVersion
	local_path := "--path=" + repoDir
	sourceFormatVersion := "1"
	err = ks.CreateRepo("filesystem", local_path, "--format-version", sourceFormatVersion)
	require.NoError(t, err)

	prev := ks.GetRepositoryStatus("Format version")

	ks.UpgradeRepository(repoDir)

	got := ks.GetRepositoryStatus("Format version")
	want := "2"

	if got != want {
		t.Errorf("Repository format version (%s) does not match expected format version (%s)", got, want)
	}
	if got == prev {
		t.Errorf("Repository format upgrade did not happen. Format version is still (%s)", prev)
	}
}
