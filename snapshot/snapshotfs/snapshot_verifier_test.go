package snapshotfs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestSnapshotVerifier(t *testing.T) {
	ctx, te := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	u := snapshotfs.NewUploader(te.RepositoryWriter)
	dir1 := mockfs.NewDirectory()

	si1 := te.LocalPathSourceInfo("/dummy/path")

	dir1.AddFile("file1", []byte{1, 2, 3}, 0o644)
	dir1.AddFile("file2", []byte{1, 2, 4}, 0o644)
	dir1.AddFile("file3", []byte{1, 2, 5}, 0o644)

	var obj1 object.ID

	require.NoError(t, repo.WriteSession(ctx, te.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		snap1, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		obj1 = snap1.RootObjectID()
		return nil
	}))

	require.NoError(t, te.RepositoryWriter.Flush(ctx))

	te2 := te.MustOpenAnother(t)

	t.Run("PositiveWithBlobMap", func(t *testing.T) {
		opts := snapshotfs.VerifierOptions{
			VerifyFilesPercent: 0,
			Parallelism:        1,
			MaxErrors:          3,
			FileQueueLength:    4,
		}

		bm, err := blob.ReadBlobMap(ctx, te.RepositoryWriter.BlobReader())
		require.NoError(t, err)

		opts.BlobMap = bm

		v := snapshotfs.NewVerifier(ctx, te2, opts)

		someErr := errors.New("some error")

		require.ErrorIs(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			return someErr
		}), someErr)

		require.ErrorIs(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			return someErr
		}), someErr)

		require.NoError(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			return nil
		}))

		require.NoError(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}))
	})

	t.Run("FullFileReadsAndBlobMap", func(t *testing.T) {
		// full verification with file reads
		opts := snapshotfs.VerifierOptions{
			VerifyFilesPercent: 100,
			MaxErrors:          30,
		}

		bm, err := blob.ReadBlobMap(ctx, te.RepositoryWriter.BlobReader())
		require.NoError(t, err)

		opts.BlobMap = bm

		v := snapshotfs.NewVerifier(ctx, te2, opts)

		require.NoError(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}))

		// now remove all 'p' blobs from the blob map
		for k := range opts.BlobMap {
			if strings.HasPrefix(string(k), "p") {
				delete(opts.BlobMap, k)
			}
		}

		require.ErrorContains(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}), "encountered 3 errors")
	})

	t.Run("MaxErrors", func(t *testing.T) {
		// now set max errors to 1 where we have 3
		opts := snapshotfs.VerifierOptions{
			MaxErrors:   1,
			Parallelism: 1,
		}

		bm, err := blob.ReadBlobMap(ctx, te.RepositoryWriter.BlobReader())
		require.NoError(t, err)

		opts.BlobMap = bm

		v := snapshotfs.NewVerifier(ctx, te2, opts)

		require.NoError(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}))

		// now remove all 'p' blobs from the blob map
		for k := range opts.BlobMap {
			if strings.HasPrefix(string(k), "p") {
				delete(opts.BlobMap, k)
			}
		}

		// we have 3 errors but max==1
		require.ErrorContains(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}), "is backed by missing blob")
	})

	t.Run("FullFileReadsNoBlobMap", func(t *testing.T) {
		opts := snapshotfs.VerifierOptions{
			VerifyFilesPercent: 100,
			MaxErrors:          30,
		}
		v := snapshotfs.NewVerifier(ctx, te2, opts)

		require.NoError(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}))

		blobs, err := blob.ListAllBlobs(ctx, te.RepositoryWriter.BlobReader(), "p")
		require.NoError(t, err)

		for _, bm := range blobs {
			require.NoError(t, te.RepositoryWriter.BlobStorage().DeleteBlob(ctx, bm.BlobID))
		}

		require.ErrorContains(t, v.InParallel(ctx, func(tw *snapshotfs.TreeWalker) error {
			tw.Process(ctx, snapshotfs.DirectoryEntry(te.Repository, obj1, nil), ".")
			return nil
		}), "encountered 3 errors")
	})
}
