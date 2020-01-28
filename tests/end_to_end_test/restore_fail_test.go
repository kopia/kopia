package endtoend_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testenv"
)

// TestRestoreFail
// Motivation: Cause a kopia snapshot restore command to fail, ensure non-zero exit code.
// Description:
//		1. Create kopia repo
//		2. Create a directory tree for testing
//		3. Issue kopia blob list before issuing any snapshots
//		4. Create a snapshot of the source directory, parse the snapshot ID
//		5. Issue another kopia blob list, find the blob IDs that were not
//			present in the previous blob list.
//		6. Find a pack blob by searching for a blob ID with the "p" prefix
//		7. Issue kopia blob delete on the ID of the found pack blob
//		8. Attempt a snapshot restore on the snapshot, expecting failure
// Pass Criteria: Kopia commands issue successfully, except the final restore
//		command is expected to fail. Expect to find new blobs after a snapshot
//		and expect one of them is a pack blob type prefixed with "p".
func TestRestoreFail(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	scratchDir := makeScratchDir(t)
	sourceDir := filepath.Join(scratchDir, "source")
	targetDir := filepath.Join(scratchDir, "target")

	testenv.MustCreateDirectoryTree(t, sourceDir, testenv.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
	})

	beforeBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	_, errOut := e.RunAndExpectSuccessWithErrOut(t, "snapshot", "create", sourceDir)
	snapID := parseSnapID(t, errOut)

	afterBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	newBlobIDs := getNewBlobIDs(beforeBlobList, afterBlobList)

	var blobIDToDelete string

	for _, blobID := range newBlobIDs {
		if strings.Contains(blobID, string(content.PackBlobIDPrefixRegular)) {
			blobIDToDelete = blobID
		}
	}

	if blobIDToDelete == "" {
		t.Fatal("Could not find a pack blob in the list of blobs created by snapshot")
	}

	// Delete a pack blob
	e.RunAndExpectSuccess(t, "blob", "delete", blobIDToDelete)

	// Expect a subsequent restore to fail
	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, targetDir)
}

func getNewBlobIDs(before, after []string) []string {
	newIDMap := make(map[string]struct{})

	// Add all blob IDs seen after the snapshot
	for _, outputStr := range after {
		blobID := parseBlobIDFromBlobList(outputStr)
		newIDMap[blobID] = struct{}{}
	}

	// Remove all blob IDs seen before the snapshot
	for _, outputStr := range before {
		blobID := parseBlobIDFromBlobList(outputStr)
		delete(newIDMap, blobID)
	}

	idList := make([]string, 0, len(newIDMap))
	for blobID := range newIDMap {
		idList = append(idList, blobID)
	}

	return idList
}

func parseBlobIDFromBlobList(str string) string {
	fields := strings.Fields(str)
	if len(fields) > 0 {
		return fields[0]
	}

	return ""
}
