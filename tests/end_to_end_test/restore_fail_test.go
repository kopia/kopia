package endtoend_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

// TestRestoreFail
// Motivation: Cause a kopia snapshot restore command to fail, ensure non-zero exit code.
// Description:
//  1. Create kopia repo
//  2. Create a directory tree for testing
//  3. Issue kopia blob list before issuing any snapshots
//  4. Create a snapshot of the source directory, parse the snapshot ID
//  5. Issue another kopia blob list, find the blob IDs that were not
//     present in the previous blob list.
//  6. Find a pack blob by searching for a blob ID with the "p" prefix
//  7. Issue kopia blob delete on the ID of the found pack blob
//  8. Attempt a snapshot restore on the snapshot, expecting failure
//
// Pass Criteria: Kopia commands issue successfully, except the final restore
// command is expected to fail. Expect to find new blobs after a snapshot
// and expect one of them is a pack blob type prefixed with "p".
func TestRestoreFail(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	scratchDir := testutil.TempDirectory(t)
	sourceDir := filepath.Join(scratchDir, "source")
	targetDir := filepath.Join(scratchDir, "target")

	testdirtree.MustCreateDirectoryTree(t, sourceDir, testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 5,
		MaxFilesPerDirectory:   5,
	}))

	beforeBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	out, errOut := e.RunAndExpectSuccessWithErrOut(t, "snapshot", "create", sourceDir)
	parsed := parseSnapshotResultFromLog(t, out, errOut)

	afterBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	newBlobIDs := getNewBlobIDs(beforeBlobList, afterBlobList)

	blobIDToDelete := findPackBlob(newBlobIDs)
	if blobIDToDelete == "" {
		t.Fatal("Could not find a pack blob in the list of blobs created by snapshot")
	}

	// Delete a pack blob
	e.RunAndExpectSuccess(t, "blob", "delete", blobIDToDelete)

	// Expect a subsequent restore to fail
	e.RunAndExpectFailure(t, "snapshot", "restore", parsed.manifestID, targetDir)

	// --ignore-errors allows the snapshot to succeed despite missing blob.
	e.RunAndExpectSuccess(t, "snapshot", "restore", "--ignore-errors", parsed.manifestID, targetDir)
}

func findPackBlob(blobIDs []string) string {
	// Pattern to match "p" followed by hexadecimal digits
	// Ex) "pd4c69d72b75a9d3d7d9da21096c6b60a"
	patternStr := fmt.Sprintf("^%s[0-9a-f]+", content.PackBlobIDPrefixRegular)
	pattern := regexp.MustCompile(patternStr)

	for _, blobID := range blobIDs {
		if pattern.MatchString(blobID) {
			return blobID
		}
	}

	return ""
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
