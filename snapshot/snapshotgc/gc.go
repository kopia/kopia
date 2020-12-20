// Package snapshotgc implements garbage collection of contents that are no longer referenced through snapshots.
package snapshotgc

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = logging.GetContextLoggerFunc("snapshotgc")

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}

func findInUseContentIDs(ctx context.Context, rep repo.Repository, used *sync.Map) error {
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

		contentIDs, err := rep.VerifyObject(ctx, oid)
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
func Run(ctx context.Context, rep *repo.DirectRepository, params maintenance.SnapshotGCParams, gcDelete bool) (Stats, error) {
	var st Stats

	err := maintenance.ReportRun(ctx, rep, "snapshot-gc", func() error {
		return runInternal(ctx, rep, params, gcDelete, &st)
	})

	return st, errors.Wrap(err, "error running snapshot gc")
}

func runInternal(ctx context.Context, rep *repo.DirectRepository, params maintenance.SnapshotGCParams, gcDelete bool, st *Stats) error {
	var (
		used sync.Map

		unused, inUse, system, tooRecent, undeleted stats.CountSum
	)

	if err := findInUseContentIDs(ctx, rep, &used); err != nil {
		return errors.Wrap(err, "unable to find in-use content ID")
	}

	log(ctx).Infof("looking for unreferenced contents")

	// Ensure that the iteration includes deleted contents, so those can be
	// undeleted (recovered).
	err := rep.Content.IterateContents(ctx, content.IterateOptions{IncludeDeleted: true}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ID.Prefix() {
			system.Add(int64(ci.Length))
			return nil
		}

		if _, ok := used.Load(ci.ID); ok {
			if ci.Deleted {
				if err := rep.Content.UndeleteContent(ctx, ci.ID); err != nil {
					return errors.Wrapf(err, "Could not undelete referenced content: %v", ci)
				}
				undeleted.Add(int64(ci.Length))
			}

			inUse.Add(int64(ci.Length))
			return nil
		}

		if rep.Time().Sub(ci.Timestamp()) < params.MinContentAge {
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

		return nil
	})

	st.UnusedCount, st.UnusedBytes = unused.Approximate()
	st.InUseCount, st.InUseBytes = inUse.Approximate()
	st.SystemCount, st.SystemBytes = system.Approximate()
	st.TooRecentCount, st.TooRecentBytes = tooRecent.Approximate()
	st.UndeletedCount, st.UndeletedBytes = undeleted.Approximate()

	if err != nil {
		return errors.Wrap(err, "error iterating contents")
	}

	if st.UnusedCount > 0 && !gcDelete {
		return errors.Errorf("Not deleting because '--delete' flag was not set")
	}

	return nil
}
