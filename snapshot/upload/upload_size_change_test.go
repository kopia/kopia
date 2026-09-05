package upload

import (
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// sizeMismatchFile is a mockfs.File that lies about its size: Size() returns
// reportedSize (the size observed during directory scan) while Open()
// streams whatever data the source provides (the data actually available at
// read time). This deterministically simulates a file changing size between
// the scan and the read, without any real filesystem or race.
type sizeMismatchFile struct {
	*mockfs.File
	reportedSize int64
}

// Size implements fs.Entry, overriding the embedded mockfs size.
func (f *sizeMismatchFile) Size() int64 {
	return f.reportedSize
}

// pseudoRandomData returns deterministic, non-trivially-compressible data.
func pseudoRandomData(n int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed)) //nolint:gosec
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(r.Intn(256))
	}

	return data
}

// TestUploadFileSizeMismatch verifies that when the size reported by
// fs.File.Size() disagrees with the length of the data actually read (the
// file changed while being snapshotted), the committed DirEntry.FileSize
// reflects the exact number of bytes written to the object — consistent with
// the object content, with DirSummary.TotalFileSize and with the multi-part
// concatenation path — rather than the stale scan-time size.
func TestUploadFileSizeMismatch(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)
	t.Cleanup(th.cleanup)

	cases := map[string]struct {
		reportedSize int64
		readData     []byte
	}{
		// Size() reports 1 MiB (stale scan-time size), but by the time the
		// data is read only 512 KiB is available (file was truncated).
		"shrink": {
			reportedSize: 1 << 20,
			readData:     pseudoRandomData(512<<10, 1),
		},
		// Size() reports 512 KiB (stale scan-time size), but reading yields
		// 1 MiB (file grew after the scan).
		"grow": {
			reportedSize: 512 << 10,
			readData:     pseudoRandomData(1<<20, 2),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &sizeMismatchFile{
				File:         mockfs.NewFile("sizechanged", tc.readData, defaultPermissions),
				reportedSize: tc.reportedSize,
			}

			// sanity: the mock must actually lie about the size.
			require.Equal(t, tc.reportedSize, f.Size(), "mock Size() must report the stale size")
			require.NotEqual(t, int64(len(tc.readData)), tc.reportedSize, "test data must actually mismatch")

			u := NewUploader(th.repo)
			policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

			man, err := u.Upload(ctx, f, policyTree, snapshot.SourceInfo{})
			require.NoError(t, err)
			require.NotNil(t, man.RootEntry)

			// integrity invariant: the committed object must contain exactly
			// the data provided by the reader (no corruption/padding/truncation).
			r, err := th.repo.OpenObject(ctx, man.RootEntry.ObjectID)
			require.NoError(t, err)

			got, err := io.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, tc.readData, got,
				"committed object content must exactly match the data provided by the reader")

			// FileSize must describe the committed object, not the stale
			// scan-time size.
			require.Equal(t, int64(len(tc.readData)), man.RootEntry.FileSize,
				"FileSize must equal the number of bytes actually written")

			require.NotNil(t, man.RootEntry.DirSummary)
			require.Equal(t, int64(len(tc.readData)), man.RootEntry.DirSummary.TotalFileSize,
				"DirSummary.TotalFileSize must stay consistent with FileSize")
		})
	}
}
