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
	err := maintenance.ReportRun(ctx, rep, maintenance.TaskSnapshotGarbageCollection, nil, func() (any, error) {
		return runInternal(ctx, rep, gcDelete, safety, maintenanceStartTime)
	})

	return errors.Wrap(err, "error running snapshot gc")
}

// SnapshotGCStats delivers the statistics for SnapshotGC
type SnapshotGCStats struct {
	UnusedCount          uint32 `json:"unusedCount"`
	UnusedSize           int64  `json:"unusedSize"`
	TooRecentUnusedCount uint32 `json:"tooRecentUnusedCount"`
	TooRecentUnusedSize  int64  `json:"tooRecentUnusedSize"`
	InUseCount           uint32 `json:"inUseCount"`
	IntUseSize           int64  `json:"intUseSize"`
	InUseSystemCount     uint32 `json:"inUseSystemCount"`
	IntUseSystemSize     int64  `json:"intUseSystemSize"`
	RecoveredCount       uint32 `json:"recoveredCount"`
	RecoveredSize        int64  `json:"recoveredSize"`
}

// WriteValueTo writes SnapshotGCStats to JSONWriter
func (ss *SnapshotGCStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField("snapshotGCStats")
	jw.UInt32Field("unusedCount", ss.UnusedCount)
	jw.Int64Field("unusedSize", ss.UnusedSize)
	jw.UInt32Field("tooRecentUnusedCount", ss.TooRecentUnusedCount)
	jw.Int64Field("tooRecentUnusedSize", ss.TooRecentUnusedSize)
	jw.UInt32Field("inUseCount", ss.InUseCount)
	jw.Int64Field("intUseSize", ss.IntUseSize)
	jw.UInt32Field("inUseSystemCount", ss.InUseSystemCount)
	jw.Int64Field("intUseSystemSize", ss.IntUseSystemSize)
	jw.UInt32Field("recoveredCount", ss.RecoveredCount)
	jw.Int64Field("recoveredSize", ss.RecoveredSize)
	jw.EndObject()
}

// MaintenanceSummary generates readable summary for SnapshotGCStats which is used by maintenance
func (ss *SnapshotGCStats) MaintenanceSummary() string {
	return fmt.Sprintf("Found %v(%v) unused contents, %v(%v) inused contents, %v(%v) inused system contents, marked %v(%v) unused countents for deletion, recovered %v(%v) contents",
		ss.UnusedCount+ss.TooRecentUnusedCount, ss.UnusedSize+ss.TooRecentUnusedSize, ss.InUseCount, ss.IntUseSize, ss.InUseSystemCount, ss.IntUseSystemSize, ss.UnusedCount, ss.UnusedSize, ss.RecoveredCount, ss.RecoveredSize)
}

func runInternal(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) (*SnapshotGCStats, error) {
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

func findUnreferencedAndRepairRereferenced(
	ctx context.Context,
	log *contentlog.Logger,
	rep repo.DirectRepositoryWriter,
	gcDelete bool,
	safety maintenance.SafetyParameters,
	maintenanceStartTime time.Time,
	used *bigmap.Set,
) (*SnapshotGCStats, error) {
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

	result := buildGCResult(&unused, &inUse, &system, &tooRecent, &undeleted)

	userLog(ctx).Infof("Snapshot GC statistics: %v", result)
	contentlog.Log1(ctx, log, "Snapshot GC", result)

	if err != nil {
		return nil, errors.Wrap(err, "error iterating contents")
	}

	if err := rep.Flush(ctx); err != nil {
		return nil, errors.Wrap(err, "flush error")
	}

	unusedCount, _ := unused.Approximate()
	if unusedCount > 0 && !gcDelete {
		return result, errors.New("Not deleting because 'gcDelete' was not set")
	}

	return result, nil
}

func buildGCResult(unused *stats.CountSum, inUse *stats.CountSum, system *stats.CountSum, tooRecent *stats.CountSum, undeleted *stats.CountSum) *SnapshotGCStats {
	result := &SnapshotGCStats{}

	cnt, size := unused.Approximate()
	result.UnusedCount = cnt
	result.UnusedSize = size

	cnt, size = tooRecent.Approximate()
	result.TooRecentUnusedCount = cnt
	result.TooRecentUnusedSize = size

	cnt, size = inUse.Approximate()
	result.InUseCount = cnt
	result.IntUseSize = size

	cnt, size = system.Approximate()
	result.InUseSystemCount = cnt
	result.IntUseSystemSize = size

	cnt, size = undeleted.Approximate()
	result.RecoveredCount = cnt
	result.RecoveredSize = size

	return result
}
