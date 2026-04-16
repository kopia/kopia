package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

func TestAllFormatsSmokeTest(t *testing.T) {
	srcDir := testutil.TempDirectory(t)

	runner := testenv.NewInProcRunner(t)

	// 3-level directory with <=10 files and <=10 subdirectories at each level
	testdirtree.CreateDirectoryTree(srcDir, testdirtree.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 5,
		MaxFilesPerDirectory:   5,
		MaxFileSize:            100,
	}, nil)

	for _, encryptionAlgo := range encryption.SupportedAlgorithms(false) {
		t.Run(encryptionAlgo, func(t *testing.T) {
			for _, hashAlgo := range hashing.SupportedAlgorithms() {
				t.Run(hashAlgo, func(t *testing.T) {
					t.Parallel()

					e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
					defer e.RunAndExpectSuccess(t, "repo", "disconnect")

					e.DefaultRepositoryCreateFlags = nil

					e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--block-hash", hashAlgo, "--encryption", encryptionAlgo)
					e.RunAndExpectSuccess(t, "snap", "create", srcDir)

					sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
					if got, want := len(sources), 1; got != want {
						t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
					}

					e.RunAndExpectSuccess(t, "repo", "disconnect")
					e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

					sources = clitestutil.ListSnapshotsAndExpectSuccess(t, e)
					if got, want := len(sources), 1; got != want {
						t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
					}
				})
			}
		})
	}
}
