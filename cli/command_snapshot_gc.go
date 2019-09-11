package cli

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	snapshotGCCommand       = snapshotCommands.Command("gc", "Remove contents not used by any snapshot")
	snapshotGCMinContentAge = snapshotGCCommand.Flag("min-age", "Minimum content age to allow deletion").Default("24h").Duration()
	snapshotGCDelete        = snapshotGCCommand.Flag("delete", "Delete unreferenced contents").Bool()
)

func findInUseContentIDs(ctx context.Context, rep *repo.Repository, used *sync.Map) error {
	w := snapshotfs.NewTreeWalker()

	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return errors.Wrap(err, "unable to load manifest IDs")
	}

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return errors.Wrap(err, "unable to get snapshot root")
		}
		w.RootEntries = append(w.RootEntries, root)
	}

	w.ObjectCallback = func(oid object.ID) error {
		_, contentIDs, err := rep.Objects.VerifyObject(ctx, oid)
		if err != nil {
			return errors.Wrapf(err, "error verifying %v", oid)
		}
		for _, cid := range contentIDs {
			used.Store(cid, nil)
		}
		return nil
	}

	log.Infof("looking for active contents")
	if err := w.Run(ctx); err != nil {
		return errors.Wrap(err, "error walking snapshot tree")
	}

	return nil
}

func runSnapshotGCCommand(ctx context.Context, rep *repo.Repository) error {
	var used sync.Map
	if err := findInUseContentIDs(ctx, rep, &used); err != nil {
		return errors.Wrap(err, "unable to find in-use content ID")
	}

	var unusedCount, inUseCount, systemCount, tooRecentCount int32
	var totalUnusedBytes, totalInUseBytes, totalSystemBytes, totalTooRecentBytes int64

	log.Infof("looking for unreferenced contents")
	if err := rep.Content.IterateContents(content.IterateOptions{}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ID.Prefix() {
			atomic.AddInt32(&systemCount, 1)
			atomic.AddInt64(&totalSystemBytes, int64(ci.Length))
			return nil
		}

		if _, ok := used.Load(ci.ID); !ok {
			if time.Since(ci.Timestamp()) < *snapshotGCMinContentAge {
				log.Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
				atomic.AddInt32(&tooRecentCount, 1)
				atomic.AddInt64(&totalTooRecentBytes, int64(ci.Length))
				return nil
			}
			log.Debugf("unreferenced %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
			cnt := atomic.AddInt32(&unusedCount, 1)
			totalSize := atomic.AddInt64(&totalUnusedBytes, int64(ci.Length))
			if *snapshotGCDelete {
				if err := rep.Content.DeleteContent(ci.ID); err != nil {
					return errors.Wrap(err, "error deleting content")
				}
			}

			if cnt%100000 == 0 {
				log.Infof("... found %v unused contents so far (%v bytes)", cnt, units.BytesStringBase2(totalSize))
				if *snapshotGCDelete {
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

	if unusedCount > 0 && !*snapshotGCDelete {
		return errors.Errorf("Not deleting because '--delete' flag was not set.")
	}

	return nil
}

func init() {
	snapshotGCCommand.Action(repositoryAction(runSnapshotGCCommand))
}
