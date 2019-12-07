package gc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = kopialogging.Logger("kopia/snapshot/gc")

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}

func findInUseContentIDs(ctx context.Context, rep *repo.Repository, used *sync.Map) error {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return errors.Wrap(err, "unable to load manifest IDs")
	}

	w := snapshotfs.NewTreeWalker()
	w.EntryID = func(e fs.Entry) interface{} { return oidOf(e) }

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return errors.Wrap(err, "unable to get snapshot root")
		}

		w.RootEntries = append(w.RootEntries, root)
	}

	w.ObjectCallback = func(entry fs.Entry) error {
		oid := oidOf(entry)
		contentIDs, err := rep.Objects.VerifyObject(ctx, oid)

		if err != nil {
			return errors.Wrapf(err, "error verifying %v", oid)
		}

		for _, cid := range contentIDs {
			used.Store(cid, nil)
		}

		return nil
	}

	log.Info("looking for active contents")

	if err := w.Run(ctx); err != nil {
		return errors.Wrap(err, "error walking snapshot tree")
	}

	return nil
}

// Run performs garbage collection on all the snapshots in the repository.
// nolint:gocognit
func Run(ctx context.Context, rep *repo.Repository, minContentAge time.Duration, gcDelete bool) error {
	var used sync.Map
	if err := findInUseContentIDs(ctx, rep, &used); err != nil {
		return errors.Wrap(err, "unable to find in-use content ID")
	}

	var unusedCount, inUseCount, systemCount, tooRecentCount int32

	var totalUnusedBytes, totalInUseBytes, totalSystemBytes, totalTooRecentBytes int64

	log.Info("looking for unreferenced contents")

	if err := rep.Content.IterateContents(content.IterateOptions{}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ID.Prefix() {
			atomic.AddInt32(&systemCount, 1)
			atomic.AddInt64(&totalSystemBytes, int64(ci.Length))
			return nil
		}

		if _, ok := used.Load(ci.ID); !ok {
			if time.Since(ci.Timestamp()) < minContentAge {
				log.Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
				atomic.AddInt32(&tooRecentCount, 1)
				atomic.AddInt64(&totalTooRecentBytes, int64(ci.Length))
				return nil
			}
			log.Debugf("unreferenced %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
			cnt := atomic.AddInt32(&unusedCount, 1)
			totalSize := atomic.AddInt64(&totalUnusedBytes, int64(ci.Length))
			if gcDelete {
				if err := rep.Content.DeleteContent(ci.ID); err != nil {
					return errors.Wrap(err, "error deleting content")
				}
			}

			if cnt%100000 == 0 {
				log.Infof("... found %v unused contents so far (%v bytes)", cnt, units.BytesStringBase2(totalSize))
				if gcDelete {
					if err := rep.Flush(ctx); err != nil {
						return errors.Wrap(err, "flush error")
					}
				}
			}
		} else {
			atomic.AddInt32(&inUseCount, 1)
			atomic.AddInt64(&totalInUseBytes, int64(ci.Length))
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	log.Infof("found %v unused contents (%v bytes)", unusedCount, units.BytesStringBase2(totalUnusedBytes))
	log.Infof("found %v unused contents that are too recent to delete (%v bytes)", tooRecentCount, units.BytesStringBase2(totalTooRecentBytes))
	log.Infof("found %v in-use contents (%v bytes)", inUseCount, units.BytesStringBase2(totalInUseBytes))
	log.Infof("found %v in-use system-contents (%v bytes)", systemCount, units.BytesStringBase2(totalSystemBytes))

	if unusedCount > 0 && !gcDelete {
		return errors.Errorf("Not deleting because '--delete' flag was not set.")
	}

	return nil
}
