package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const snapshotGCStatsKind = "snapshotGCStats"

// SnapshotGCStats delivers are the stats for snapshot GC.
type SnapshotGCStats struct {
	UnreferencedContentCount uint32 `json:"unreferencedContentCount"`
	UnreferencedContentSize  int64  `json:"unreferencedContentSize"`
	DeletedContentCount      uint32 `json:"deletedContentCount"`
	DeletedContentSize       int64  `json:"deletedContentSize"`
	RetainedContentCount     uint32 `json:"retainedContentCount"`
	RetainedContentSize      int64  `json:"retainedContentSize"`
	InUseContentCount        uint32 `json:"inUseContentCount"`
	InUseContentSize         int64  `json:"inUseContentSize"`
	InUseSystemContentCount  uint32 `json:"inUseSystemContentCount"`
	InUseSystemContentSize   int64  `json:"inUseSystemContentSize"`
	RecoveredContentCount    uint32 `json:"recoveredContentCount"`
	RecoveredContentSize     int64  `json:"recoveredContentSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (ss *SnapshotGCStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ss.Kind())
	jw.UInt32Field("unreferencedContentCount", ss.UnreferencedContentCount)
	jw.Int64Field("unreferencedContentSize", ss.UnreferencedContentSize)
	jw.UInt32Field("deletedContentCount", ss.DeletedContentCount)
	jw.Int64Field("deletedContentSize", ss.DeletedContentSize)
	jw.UInt32Field("retainedContentCount", ss.RetainedContentCount)
	jw.Int64Field("retainedContentSize", ss.RetainedContentSize)
	jw.UInt32Field("inUseContentCount", ss.InUseContentCount)
	jw.Int64Field("inUseContentSize", ss.InUseContentSize)
	jw.UInt32Field("inUseSystemContentCount", ss.InUseSystemContentCount)
	jw.Int64Field("inUseSystemContentSize", ss.InUseSystemContentSize)
	jw.UInt32Field("recoveredContentCount", ss.RecoveredContentCount)
	jw.Int64Field("recoveredContentSize", ss.RecoveredContentSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (ss *SnapshotGCStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unreferenced contents and marked %v(%v) for deletion. Found %v(%v) inused contents and %v(%v) inused system contents. Retained %v(%v) unused contents. Recovered %v(%v) contents",
		ss.UnreferencedContentCount, ss.UnreferencedContentSize, ss.DeletedContentCount, ss.DeletedContentSize, ss.InUseContentCount, ss.InUseContentSize,
		ss.InUseSystemContentCount, ss.InUseSystemContentSize, ss.RetainedContentCount, ss.RetainedContentSize, ss.RecoveredContentCount, ss.RecoveredContentSize)
}

// Kind returns the kind name for the stats.
func (ss *SnapshotGCStats) Kind() string {
	return snapshotGCStatsKind
}
