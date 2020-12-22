package kopiarunner

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestParseSnapListAllExeTest(t *testing.T) {
	baseDir, err := ioutil.TempDir("", t.Name())
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(baseDir)

	repoDir, err := ioutil.TempDir(baseDir, "repo")
	testenv.AssertNoError(t, err)

	sourceDir, err := ioutil.TempDir(baseDir, "source")
	testenv.AssertNoError(t, err)

	ks, err := NewKopiaSnapshotter(repoDir)
	if errors.Is(err, ErrExeVariableNotSet) {
		t.Skip("KOPIA_EXE not set, skipping test")
	}

	testenv.AssertNoError(t, err)

	err = ks.ConnectOrCreateFilesystem(repoDir)
	testenv.AssertNoError(t, err)

	// Empty snapshot list
	snapIDListSnap, err := ks.snapIDsFromSnapListAll()
	testenv.AssertNoError(t, err)

	if got, want := len(snapIDListSnap), 0; got != want {
		t.Errorf("Snapshot list (len %d) should be empty", got)
	}

	fmt.Println(snapIDIsLastInList("asdf", snapIDListSnap))

	const numSnapsToTest = 5
	for snapCount := 0; snapCount < numSnapsToTest; snapCount++ {
		snapID, err := ks.CreateSnapshot(sourceDir)
		testenv.AssertNoError(t, err)

		// Validate the list against kopia snapshot list --all
		snapIDListSnap, err := ks.snapIDsFromSnapListAll()
		testenv.AssertNoError(t, err)

		if got, want := len(snapIDListSnap), snapCount+1; got != want {
			t.Errorf("Snapshot list len (%d) does not match expected number of snapshots (%d)", got, want)
		}

		if !snapIDIsLastInList(snapID, snapIDListSnap) {
			t.Errorf("Snapshot ID that was just created %s was not in the snapshot list", snapID)
		}

		// Validate the list against kopia snapshot list --all
		snapIDListMan, err := ks.snapIDsFromManifestList()
		testenv.AssertNoError(t, err)

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
