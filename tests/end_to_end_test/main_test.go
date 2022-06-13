package endtoend_test

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testdirtree"
)

var (
	sharedTestDataDirBase string
	sharedTestDataDir1    string
	sharedTestDataDir2    string
	sharedTestDataDir3    string
)

func oneTimeSetup() error {
	var err error

	sharedTestDataDirBase, err = testutil.GetInterestingTempDirectoryName()
	if err != nil {
		return errors.Wrap(err, "unable to create data directory")
	}

	// if enabled, make sure the base directory is quite long to trigger very long filenames on Windows
	// skipped during race detection, on ARM, and by default to keep logs cleaner
	if !testutil.ShouldSkipLongFilenames() {
		if n, targetLen := len(sharedTestDataDirBase), 270; n < targetLen {
			sharedTestDataDirBase = filepath.Join(sharedTestDataDirBase, strings.Repeat("f", targetLen-n))
			os.MkdirAll(sharedTestDataDirBase, 0o700)
		}
	}

	var counters1, counters2, counters3 testdirtree.DirectoryTreeCounters

	sharedTestDataDir1 = filepath.Join(sharedTestDataDirBase, "dir1")
	// 3-level directory with <=10 files and <=10 subdirectories at each level
	testdirtree.CreateDirectoryTree(sharedTestDataDir1, testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                  3,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}), &counters1)

	// directory with very few big files
	sharedTestDataDir2 = filepath.Join(sharedTestDataDirBase, "dir2")
	testdirtree.CreateDirectoryTree(sharedTestDataDir2, testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                  3,
		MaxSubdirsPerDirectory: 2,
		MaxFilesPerDirectory:   2,
		MaxFileSize:            50000000,
	}), &counters2)

	sharedTestDataDir3 = filepath.Join(sharedTestDataDirBase, "dir3")
	testdirtree.CreateDirectoryTree(sharedTestDataDir3, testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                  3,
		MaxFilesPerDirectory:   500,
		MaxSubdirsPerDirectory: 3,
		MaxFileSize:            500,
	}), &counters3)

	return nil
}

func oneTimeCleanup() {
	if sharedTestDataDirBase != "" {
		os.RemoveAll(sharedTestDataDirBase)
	}
}

func TestMain(m *testing.M) {
	if err := oneTimeSetup(); err != nil {
		log.Fatalf("error setting up test: %v", err)
	}

	testutil.MyTestMain(m, oneTimeCleanup)
}
