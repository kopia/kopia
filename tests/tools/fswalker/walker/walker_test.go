// +build linux

package walker

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/testenv"
)

func TestWalk(t *testing.T) {
	dataDir, err := ioutil.TempDir("", "walk-data-")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(dataDir)

	counters := new(testenv.DirectoryTreeCounters)
	err = testenv.CreateDirectoryTree(
		dataDir,
		testenv.DirectoryTreeOptions{
			Depth:                  2,
			MaxSubdirsPerDirectory: 2,
			MaxFilesPerDirectory:   2,
		},
		counters,
	)
	testenv.AssertNoError(t, err)

	walk, err := Walk(context.TODO(),
		&fspb.Policy{
			Include: []string{
				dataDir,
			},
			WalkCrossDevice: true,
		})
	testenv.AssertNoError(t, err)

	fileList := walk.GetFile()
	if got, want := len(fileList), counters.Files+counters.Directories; got != want {
		t.Errorf("Expected number of walk entries (%v) to equal sum of file and dir counts (%v)", got, want)
	}
}

func TestWalkFail(t *testing.T) {
	_, err := Walk(
		context.TODO(),
		&fspb.Policy{
			Include: []string{
				"some/nonexistent/directory",
			},
		},
	)
	if err == nil {
		t.Fatalf("Expected non-nil error when walk directory is not present")
	}

	if !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("Expected walk call to return an error for finding no directory but got %q", err.Error())
	}
}
