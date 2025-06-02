package kopiarunner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapListAllExeTest(t *testing.T) {
	repoDir := t.TempDir()
	sourceDir := t.TempDir()

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
	require.Empty(t, snapIDListSnap, "Snapshot list should be empty")

	const numSnapsToTest = 5
	for snapCount := range numSnapsToTest {
		snapID, err := ks.CreateSnapshot(sourceDir)
		require.NoError(t, err)

		// Validate the list against kopia snapshot list --all
		snapIDListSnap, err := ks.snapIDsFromSnapListAll()

		require.NoError(t, err)
		require.Len(t, snapIDListSnap, snapCount+1, "snapshot list length does not match expected number of snapshots")
		require.Truef(t, snapIDIsLastInList(snapID, snapIDListSnap), "snapshot ID that was just created was not in the snapshot list %s", snapID)

		// Validate the list against kopia snapshot list --all
		snapIDListMan, err := ks.snapIDsFromManifestList()

		require.NoError(t, err)
		require.Len(t, snapIDListMan, snapCount+1, "snapshot list length does not match expected number of snapshots")
		require.True(t, snapIDIsLastInList(snapID, snapIDListMan), "snapshot ID that was just created was not in the snapshot list", snapID)
	}
}

func snapIDIsLastInList(snapID string, snapIDList []string) bool {
	return len(snapIDList) > 0 && snapIDList[len(snapIDList)-1] == snapID
}
