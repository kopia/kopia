// Package snapshotgc implements garbage collection of contents that are no longer referenced through snapshots.
package snapshotgc

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/bigmap"
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

var log = logging.Module("snapshotgc")

func findInUseContentIDs(ctx context.Context, rep repo.Repository, used *bigmap.Set) error {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
	if err != nil {
		return errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return errors.Wrap(err, "unable to load manifest IDs")
	}

	w, twerr := snapshotfs.NewTreeWalker(ctx, snapshotfs.TreeWalkerOptions{
		EntryCallback: func(ctx context.Context, _ fs.Entry, oid object.ID, _ string) error {
			contentIDs, verr := rep.VerifyObject(ctx, oid)
			if verr != nil {
				return errors.Wrapf(verr, "error verifying %v", oid)
			}

			var cidbuf [128]byte

			for _, cid := range contentIDs {
				used.Put(ctx, cid.Append(cidbuf[:0]))
			}

			return nil
		},
	})
	if twerr != nil {
		return errors.Wrap(err, "unable to create tree walker")
	}

	defer w.Close(ctx)

	log(ctx).Info("Looking for active contents...")

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return errors.Wrap(err, "unable to get snapshot root")
		}

		if err := w.Process(ctx, root, ""); err != nil {
			return errors.Wrap(err, "error processing snapshot root")
		}
	}

	return nil
}

// Run performs garbage collection on all the snapshots in the repository.
func Run(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) (Stats, error) {
	var st Stats

	err := maintenance.ReportRun(ctx, rep, maintenance.TaskSnapshotGarbageCollection, nil, func() error {
		if err := runInternal(ctx, rep, gcDelete, safety, maintenanceStartTime, &st); err != nil {
			return err
		}

		l := log(ctx)

		l.Infof("GC found %v unused contents (%v)", st.UnusedCount, units.BytesString(st.UnusedBytes))
		l.Infof("GC found %v unused contents that are too recent to delete (%v)", st.TooRecentCount, units.BytesString(st.TooRecentBytes))
		l.Infof("GC found %v in-use contents (%v)", st.InUseCount, units.BytesString(st.InUseBytes))
		l.Infof("GC found %v in-use system-contents (%v)", st.SystemCount, units.BytesString(st.SystemBytes))

		if st.UnusedCount > 0 && !gcDelete {
			return errors.New("Not deleting because 'gcDelete' was not set")
		}

		return nil
	})

	return st, errors.Wrap(err, "error running snapshot gc")
}

func runInternal(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time, st *Stats) error {
	var unused, inUse, system, tooRecent, undeleted stats.CountSum

	used, serr := bigmap.NewSet(ctx)
	if serr != nil {
		return errors.Wrap(serr, "unable to create new set")
	}
	defer used.Close(ctx)

	if err := findInUseContentIDs(ctx, rep, used); err != nil {
		return errors.Wrap(err, "unable to find in-use content ID")
	}

	log(ctx).Info("Looking for unreferenced contents...")

	// Ensure that the iteration includes deleted contents, so those can be
	// undeleted (recovered).
	err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{IncludeDeleted: true}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ContentID.Prefix() {
			system.Add(int64(ci.PackedLength))
			return nil
		}

		var cidbuf [128]byte

		if used.Contains(ci.ContentID.Append(cidbuf[:0])) {
			if ci.Deleted {
				if err := rep.ContentManager().UndeleteContent(ctx, ci.ContentID); err != nil {
					return errors.Wrapf(err, "Could not undelete referenced content: %v", ci)
				}

				undeleted.Add(int64(ci.PackedLength))
			}

			inUse.Add(int64(ci.PackedLength))

			return nil
		}

		if maintenanceStartTime.Sub(ci.Timestamp()) < safety.MinContentAgeSubjectToGC {
			log(ctx).Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ContentID, ci.PackedLength, ci.Timestamp())
			tooRecent.Add(int64(ci.PackedLength))

			return nil
		}

		log(ctx).Debugf("unreferenced %v (%v bytes, modified %v)", ci.ContentID, ci.PackedLength, ci.Timestamp())
		cnt, totalSize := unused.Add(int64(ci.PackedLength))

		if gcDelete {
			if err := rep.ContentManager().DeleteContent(ctx, ci.ContentID); err != nil {
				return errors.Wrap(err, "error deleting content")
			}
		}

		if cnt%100000 == 0 {
			log(ctx).Infof("... found %v unused contents so far (%v bytes)", cnt, units.BytesString(totalSize))

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

	return errors.Wrap(rep.Flush(ctx), "flush error")
}
