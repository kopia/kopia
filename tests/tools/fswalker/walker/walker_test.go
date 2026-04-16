//go:build darwin || (linux && amd64)

package walker

import (
	"os"
	"strings"
	"testing"

	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/testdirtree"
)

func TestWalk(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "walk-data-")
	require.NoError(t, err)

	defer os.RemoveAll(dataDir)

	counters := new(testdirtree.DirectoryTreeCounters)
	err = testdirtree.CreateDirectoryTree(
		dataDir,
		testdirtree.DirectoryTreeOptions{
			Depth:                  2,
			MaxSubdirsPerDirectory: 2,
			MaxFilesPerDirectory:   2,
		},
		counters,
	)
	require.NoError(t, err)

	ctx := testlogging.Context(t)

	walk, err := Walk(ctx,
		&fspb.Policy{
			Include: []string{
				dataDir,
			},
			WalkCrossDevice: true,
		})
	require.NoError(t, err)

	fileList := walk.GetFile()
	if got, want := len(fileList), counters.Files+counters.Directories; got != want {
		t.Errorf("Expected number of walk entries (%v) to equal sum of file and dir counts (%v)", got, want)
	}
}

func TestWalkFail(t *testing.T) {
	ctx := testlogging.Context(t)

	_, err := Walk(
		ctx,
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
