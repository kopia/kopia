package testutil

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

var interestingLengths = []int{10, 50, 100, 240, 250, 260, 270}

// GetInterestingTempDirectoryName returns interesting directory name used for testing.
func GetInterestingTempDirectoryName() (string, error) {
	td, err := ioutil.TempDir("", "kopia-test")
	if err != nil {
		return "", errors.Wrap(err, "unable to create temp directory")
	}

	// nolint:gosec
	targetLen := interestingLengths[rand.Intn(len(interestingLengths))]

	// make sure the base directory is quite long to trigger very long filenames on Windows.
	if n := len(td); n < targetLen {
		td = filepath.Join(td, strings.Repeat("f", targetLen-n))
		// nolint:gomnd
		if err := os.MkdirAll(td, 0700); err != nil {
			return "", errors.Wrap(err, "unable to create temp directory")
		}
	}

	return td, nil
}

// TempDirectory returns an interesting temporary directory and cleans it up before test
// completes.
func TempDirectory(t *testing.T) string {
	t.Helper()

	d, err := GetInterestingTempDirectoryName()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if !t.Failed() {
			os.RemoveAll(d) // nolint:errcheck
		} else {
			t.Logf("temporary files left in %v", d)
		}
	})

	return d
}
