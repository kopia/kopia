package endtoend_test

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/tests/testenv"
)

var (
	sharedTestDataDirBase string
	sharedTestDataDir1    string
	sharedTestDataDir2    string
	sharedTestDataDir3    string
)

func oneTimeSetup() error {
	var err error

	sharedTestDataDirBase, err = ioutil.TempDir("", "kopia-test")
	if err != nil {
		return errors.Wrap(err, "unable to create data directory")
	}

	log.Printf("creating test data in %q", sharedTestDataDirBase)

	var counters1, counters2, counters3 testenv.DirectoryTreeCounters

	sharedTestDataDir1 = filepath.Join(sharedTestDataDirBase, "dir1")
	// 3-level directory with <=10 files and <=10 subdirectories at each level
	testenv.CreateDirectoryTree(sharedTestDataDir1, testenv.DirectoryTreeOptions{
		Depth:                  3,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
		MaxFileSize:            100,
	}, &counters1)
	log.Printf("created dir1 with %#v", counters1)

	// directory with very few big files
	sharedTestDataDir2 = filepath.Join(sharedTestDataDirBase, "dir2")
	testenv.CreateDirectoryTree(sharedTestDataDir2, testenv.DirectoryTreeOptions{
		Depth:                  5,
		MaxSubdirsPerDirectory: 2,
		MaxFilesPerDirectory:   2,
		MaxFileSize:            50000000,
	}, &counters2)
	log.Printf("created dir2 with %#v", counters2)

	sharedTestDataDir3 = filepath.Join(sharedTestDataDirBase, "dir3")
	testenv.CreateDirectoryTree(sharedTestDataDir3, testenv.DirectoryTreeOptions{
		Depth:                  3,
		MaxFilesPerDirectory:   500,
		MaxSubdirsPerDirectory: 3,
		MaxFileSize:            500,
	}, &counters3)
	log.Printf("created dir3 with %#v", counters3)

	log.Printf("finished creating test data...")

	return nil
}

func randomString(n int) string {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b)

	return hex.EncodeToString(b)
}

func makeScratchDir(t *testing.T) string {
	baseTestName := strings.Split(t.Name(), "/")[0]
	d := filepath.Join(sharedTestDataDirBase, baseTestName, randomString(4))

	if err := os.MkdirAll(d, 0o700); err != nil {
		t.Fatalf("unable to make scratch dir: %v", err)
	}

	return d
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

	result := m.Run()

	oneTimeCleanup()
	os.Exit(result)
}
