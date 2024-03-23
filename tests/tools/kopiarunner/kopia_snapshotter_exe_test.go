package kopiarunner

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapListAllExeTest(t *testing.T) {
	baseDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(baseDir)

	repoDir, err := os.MkdirTemp(baseDir, "repo")
	require.NoError(t, err)

	sourceDir, err := os.MkdirTemp(baseDir, "source")
	require.NoError(t, err)

	ks, err := NewKopiaSnapshotter(repoDir)
	if errors.Is(err, ErrExeVariableNotSet) {
		t.Skip("KOPIA_EXE not set, skipping test")
	}

	require.NoError(t, err)

	err = ks.ConnectOrCreateFilesystem(repoDir)
	require.NoError(t, err)

	// Empty snapshot list
	snapIDListSnap, err := ks.snapIDsFromSnapListAll()
	require.NoError(t, err)

	if got, want := len(snapIDListSnap), 0; got != want {
		t.Errorf("Snapshot list (len %d) should be empty", got)
	}

	fmt.Println(snapIDIsLastInList("asdf", snapIDListSnap))

	const numSnapsToTest = 5
	for snapCount := range numSnapsToTest {
		snapID, err := ks.CreateSnapshot(sourceDir)
		require.NoError(t, err)

		// Validate the list against kopia snapshot list --all
		snapIDListSnap, err := ks.snapIDsFromSnapListAll()
		require.NoError(t, err)

		if got, want := len(snapIDListSnap), snapCount+1; got != want {
			t.Errorf("Snapshot list len (%d) does not match expected number of snapshots (%d)", got, want)
		}

		if !snapIDIsLastInList(snapID, snapIDListSnap) {
			t.Errorf("Snapshot ID that was just created %s was not in the snapshot list", snapID)
		}

		// Validate the list against kopia snapshot list --all
		snapIDListMan, err := ks.snapIDsFromManifestList()
		require.NoError(t, err)

		if got, want := len(snapIDListMan), snapCount+1; got != want {
			t.Errorf("Snapshot list len (%d) does not match expected number of snapshots (%d)", got, want)
		}

		if !snapIDIsLastInList(snapID, snapIDListSnap) {
			t.Errorf("Snapshot ID that was just created %s was not in the manifest list", snapID)
		}
	}
}

func snapIDIsLastInList(snapID string, snapIDList []string) bool {
	return len(snapIDList) > 0 && snapIDList[len(snapIDList)-1] == snapID
}
