// Package gc implements garbage collection of contents that are no longer referenced through snapshots.
package gc

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = logging.GetContextLoggerFunc("kopia/snapshot/gc")

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

	log(ctx).Infof("looking for active contents")

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

	var unused, inUse, system, tooRecent stats.CountSum

	log(ctx).Infof("looking for unreferenced contents")

	if err := rep.Content.IterateContents(ctx, content.IterateOptions{}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ID.Prefix() {
			system.Add(int64(ci.Length))
			return nil
		}

		if _, ok := used.Load(ci.ID); !ok {
			if rep.Time().Sub(ci.Timestamp()) < minContentAge {
				log(ctx).Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
				tooRecent.Add(int64(ci.Length))
				return nil
			}

			log(ctx).Debugf("unreferenced %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
			cnt, totalSize := unused.Add(int64(ci.Length))

			if gcDelete {
				if err := rep.Content.DeleteContent(ctx, ci.ID); err != nil {
					return errors.Wrap(err, "error deleting content")
				}
			}

			if cnt%100000 == 0 {
				log(ctx).Infof("... found %v unused contents so far (%v bytes)", cnt, units.BytesStringBase2(totalSize))
				if gcDelete {
					if err := rep.Flush(ctx); err != nil {
						return errors.Wrap(err, "flush error")
					}
				}
			}
		} else {
			inUse.Add(int64(ci.Length))
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	unusedCount, totalBytes := unused.Approximate()
	log(ctx).Infof("found %v unused contents (%v bytes)", unusedCount, units.BytesStringBase2(totalBytes))
	cnt, totalBytes := tooRecent.Approximate()
	log(ctx).Infof("found %v unused contents that are too recent to delete (%v bytes)", cnt, units.BytesStringBase2(totalBytes))
	cnt, totalBytes = inUse.Approximate()
	log(ctx).Infof("found %v in-use contents (%v bytes)", cnt, units.BytesStringBase2(totalBytes))
	cnt, totalBytes = system.Approximate()
	log(ctx).Infof("found %v in-use system-contents (%v bytes)", cnt, units.BytesStringBase2(totalBytes))

	if unusedCount > 0 && !gcDelete {
		return errors.Errorf("Not deleting because '--delete' flag was not set")
	}

	return nil
}
