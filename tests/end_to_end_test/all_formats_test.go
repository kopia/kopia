package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/tests/testenv"
)

func TestAllFormatsSmokeTest(t *testing.T) {
	for _, encryptionAlgo := range encryption.SupportedAlgorithms(false) {
		encryptionAlgo := encryptionAlgo

		t.Run(encryptionAlgo, func(t *testing.T) {
			for _, hashAlgo := range hashing.SupportedAlgorithms() {

				hashAlgo := hashAlgo
				t.Run(hashAlgo, func(t *testing.T) {
					t.Parallel()

					e := testenv.NewCLITest(t)
					defer e.Cleanup(t)
					defer e.RunAndExpectSuccess(t, "repo", "disconnect")

					e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--block-hash", hashAlgo, "--encryption", encryptionAlgo)
					e.RunAndExpectSuccess(t, "snap", "create", sharedTestDataDir1)

					sources := e.ListSnapshotsAndExpectSuccess(t)
					if got, want := len(sources), 1; got != want {
						t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
					}

					e.RunAndExpectSuccess(t, "repo", "disconnect")
					e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

					sources = e.ListSnapshotsAndExpectSuccess(t)
					if got, want := len(sources), 1; got != want {
						t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
					}
				})
			}
		})
	}
}
