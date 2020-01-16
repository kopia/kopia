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
	sharedTestDataDir1 = filepath.Join(sharedTestDataDirBase, "dir1")
	testenv.CreateDirectoryTree(sharedTestDataDir1, 3)

	sharedTestDataDir2 = filepath.Join(sharedTestDataDirBase, "dir2")
	testenv.CreateDirectoryTree(sharedTestDataDir2, 3)

	sharedTestDataDir3 = filepath.Join(sharedTestDataDirBase, "dir3")
	testenv.CreateDirectoryTree(sharedTestDataDir3, 3)
	log.Printf("finished creating test data...")

	return nil
}

func randomString(n int) string {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b) //nolint:errcheck

	return hex.EncodeToString(b)
}

func makeScratchDir(t *testing.T) string {
	baseTestName := strings.Split(t.Name(), "/")[0]
	d := filepath.Join(sharedTestDataDirBase, baseTestName, randomString(8))

	if err := os.MkdirAll(d, 0700); err != nil {
		t.Fatalf("unable to make scratch dir: %v", err)
	}

	return d
}

func oneTimeCleanup() {
	if sharedTestDataDirBase != "" {
		os.RemoveAll(sharedTestDataDirBase) //nolint:errcheck
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
