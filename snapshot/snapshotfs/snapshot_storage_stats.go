package snapshotfs

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// CalculateStorageStats calculates the storage statistics for a given list of snapshots,
// by determining the count and size of unique contents and objects for each snapshot in
// the slice as well as running total so far for all the previous snapshots.
//
// For each snapshot the provided callback is invoked as soon as the results
// are available and iteration continues until the callback returns a non-nil error.
func CalculateStorageStats(ctx context.Context, rep repo.Repository, manifests []*snapshot.Manifest, callback func(m *snapshot.Manifest) error) error {
	unique := new(snapshot.StorageUsageDetails)
	runningTotal := new(snapshot.StorageUsageDetails)

	uniqueContents, err := bigmap.NewSet(ctx)
	if err != nil {
		return errors.Wrap(err, "NewSet")
	}

	defer uniqueContents.Close(ctx)

	tw, twerr := NewTreeWalker(ctx, TreeWalkerOptions{
		EntryCallback: func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error {
			_ = entryPath

			if !entry.IsDir() {
				atomic.AddInt32(&unique.FileObjectCount, 1)
				atomic.AddInt32(&runningTotal.FileObjectCount, 1)

				atomic.AddInt64(&unique.ObjectBytes, entry.Size())
				atomic.AddInt64(&runningTotal.ObjectBytes, entry.Size())
			} else {
				atomic.AddInt32(&unique.DirObjectCount, 1)
				atomic.AddInt32(&runningTotal.DirObjectCount, 1)
			}

			contentIDs, err := rep.VerifyObject(ctx, oid)
			if err != nil {
				return errors.Wrapf(err, "error verifying object %v", oid)
			}

			var cidbuf [128]byte

			for _, cid := range contentIDs {
				if uniqueContents.Put(ctx, cid.Append(cidbuf[:0])) {
					atomic.AddInt32(&unique.ContentCount, 1)
					atomic.AddInt32(&runningTotal.ContentCount, 1)

					if !entry.IsDir() {
						info, err := rep.ContentInfo(ctx, cid)
						if err != nil {
							return errors.Wrapf(err, "error getting content info for %v", cid)
						}

						l := int64(info.OriginalLength)

						atomic.AddInt64(&unique.OriginalContentBytes, l)
						atomic.AddInt64(&runningTotal.OriginalContentBytes, l)

						l2 := int64(info.PackedLength)

						atomic.AddInt64(&unique.PackedContentBytes, l2)
						atomic.AddInt64(&runningTotal.PackedContentBytes, l2)
					}
				}
			}

			return nil
		},
	})
	if twerr != nil {
		return errors.Wrap(twerr, "tree walker")
	}
	defer tw.Close(ctx)

	src := manifests[0].Source

	for _, snap := range manifests {
		*unique = snapshot.StorageUsageDetails{}

		rootName := src.String() + "@" + snap.StartTime.Format(time.RFC3339)

		root, err := SnapshotRoot(rep, snap)
		if err != nil {
			return errors.Wrapf(err, "unable to get snapshot root for %v", rootName)
		}

		if err := tw.Process(ctx, root, rootName); err != nil {
			return errors.Wrapf(err, "error processing %v", rootName)
		}

		snap.StorageStats = &snapshot.StorageStats{
			NewData:      *unique,
			RunningTotal: *runningTotal,
		}

		if err := callback(snap); err != nil {
			return err
		}
	}

	return nil
}
