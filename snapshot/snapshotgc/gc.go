// Package snapshotgc implements garbage collection of contents that are no longer referenced through snapshots.
package snapshotgc

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
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

// User-visible log output.
var userLog = logging.Module("snapshotgc")

func findInUseContentIDs(ctx context.Context, log *contentlog.Logger, rep repo.Repository, used *bigmap.Set) error {
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
		return errors.Wrap(twerr, "unable to create tree walker")
	}

	defer w.Close(ctx)

	contentlog.Log(ctx, log, "Looking for active contents...")

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
func Run(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) error {
	err := maintenance.ReportRun(ctx, rep, maintenance.TaskSnapshotGarbageCollection, nil, func() (string, error) {
		result, err := runInternal(ctx, rep, gcDelete, safety, maintenanceStartTime)
		if err != nil {
			return "", err
		}

		message := ""
		if result != nil {
			message = fmt.Sprintf("GC found %v(%v) unused contents, %v(%v) inused contents, marked %v(%v) unused countents for deletion, recovered %v(%v) contents",
				result.unUsedCount, result.unUsedSize, result.inUseCount, result.intUseSize, result.deletedCount, result.deletedSize, result.recoveredCount, result.recoveredSize)
		}

		return message, nil
	})

	return errors.Wrap(err, "error running snapshot gc")
}

type gcResult struct {
	unUsedCount    uint32
	unUsedSize     int64
	deletedCount   uint32
	deletedSize    int64
	inUseCount     uint32
	intUseSize     int64
	recoveredCount uint32
	recoveredSize  int64
}

func runInternal(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) (*gcResult, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:snapshot-gc", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-snapshot-gc")

	used, serr := bigmap.NewSet(ctx)
	if serr != nil {
		return nil, errors.Wrap(serr, "unable to create new set")
	}
	defer used.Close(ctx)

	if err := findInUseContentIDs(ctx, log, rep, used); err != nil {
		return nil, errors.Wrap(err, "unable to find in-use content ID")
	}

	return findUnreferencedAndRepairRereferenced(ctx, log, rep, gcDelete, safety, maintenanceStartTime, used)
}

//nolint:funlen
func findUnreferencedAndRepairRereferenced(
	ctx context.Context,
	log *contentlog.Logger,
	rep repo.DirectRepositoryWriter,
	gcDelete bool,
	safety maintenance.SafetyParameters,
	maintenanceStartTime time.Time,
	used *bigmap.Set,
) (*gcResult, error) {
	var unused, inUse, system, tooRecent, undeleted stats.CountSum

	contentlog.Log(ctx, log, "Looking for unreferenced contents...")

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
			contentlog.Log3(ctx, log,
				"recent unreferenced content",
				contentparam.ContentID("contentID", ci.ContentID),
				logparam.Int64("bytes", int64(ci.PackedLength)),
				logparam.Time("modified", ci.Timestamp()))
			tooRecent.Add(int64(ci.PackedLength))

			return nil
		}

		contentlog.Log3(ctx, log,
			"unreferenced content",
			contentparam.ContentID("contentID", ci.ContentID),
			logparam.Int64("bytes", int64(ci.PackedLength)),
			logparam.Time("modified", ci.Timestamp()))

		cnt, totalSize := unused.Add(int64(ci.PackedLength))

		if gcDelete {
			if err := rep.ContentManager().DeleteContent(ctx, ci.ContentID); err != nil {
				return errors.Wrap(err, "error deleting content")
			}
		}

		if cnt%100000 == 0 {
			contentlog.Log2(ctx, log,
				"found unused contents so far",
				logparam.UInt32("count", cnt),
				logparam.Int64("bytes", totalSize))

			if gcDelete {
				if err := rep.Flush(ctx); err != nil {
					return errors.Wrap(err, "flush error")
				}
			}
		}

		return nil
	})

	unusedCount, unusedBytes := unused.Approximate()
	inUseCount, inUseBytes := inUse.Approximate()
	systemCount, systemBytes := system.Approximate()
	tooRecentCount, tooRecentBytes := tooRecent.Approximate()
	undeletedCount, undeletedBytes := undeleted.Approximate()

	userLog(ctx).Infof("GC found %v unused contents (%v)", unusedCount, units.BytesString(unusedBytes))
	userLog(ctx).Infof("GC found %v unused contents that are too recent to delete (%v)", tooRecentCount, units.BytesString(tooRecentBytes))
	userLog(ctx).Infof("GC found %v in-use contents (%v)", inUseCount, units.BytesString(inUseBytes))
	userLog(ctx).Infof("GC found %v in-use system-contents (%v)", systemCount, units.BytesString(systemBytes))
	userLog(ctx).Infof("GC undeleted %v contents (%v)", undeletedCount, units.BytesString(undeletedBytes))

	contentlog.Log2(ctx, log,
		"GC found unused contents",
		logparam.UInt32("count", unusedCount),
		logparam.Int64("bytes", unusedBytes))
	contentlog.Log2(ctx, log,
		"GC found unused contents that are too recent to delete",
		logparam.UInt32("count", tooRecentCount),
		logparam.Int64("bytes", tooRecentBytes))
	contentlog.Log2(ctx, log,
		"GC found in-use contents",
		logparam.UInt32("count", inUseCount),
		logparam.Int64("bytes", inUseBytes))
	contentlog.Log2(ctx, log,
		"GC found in-use system-contents",
		logparam.UInt32("count", systemCount),
		logparam.Int64("bytes", systemBytes))
	contentlog.Log2(ctx, log,
		"GC undeleted contents",
		logparam.UInt32("count", undeletedCount),
		logparam.Int64("bytes", undeletedBytes))

	if err != nil {
		return nil, errors.Wrap(err, "error iterating contents")
	}

	if err := rep.Flush(ctx); err != nil {
		return nil, errors.Wrap(err, "flush error")
	}

	result := buildGCResult(&unused, &inUse, &system, &tooRecent, &undeleted, gcDelete)

	if unusedCount > 0 && !gcDelete {
		return result, errors.New("Not deleting because 'gcDelete' was not set")
	}

	return result, nil
}

func buildGCResult(unused *stats.CountSum, inUse *stats.CountSum, system *stats.CountSum, tooRecent *stats.CountSum, undeleted *stats.CountSum, delete bool) *gcResult {
	result := &gcResult{}

	cnt, size := unused.Approximate()
	result.unUsedCount = cnt
	result.unUsedSize = size

	if delete {
		result.deletedCount = cnt
		result.deletedSize = size
	}

	cnt, size = tooRecent.Approximate()
	result.unUsedCount += cnt
	result.unUsedSize += size

	cnt, size = inUse.Approximate()
	result.inUseCount = cnt
	result.intUseSize = size

	cnt, size = system.Approximate()
	result.inUseCount += cnt
	result.intUseSize += size

	cnt, size = undeleted.Approximate()
	result.recoveredCount = cnt
	result.recoveredSize = size

	return result
}
