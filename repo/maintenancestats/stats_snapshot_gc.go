package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const snapshotGCStatsKind = "snapshotGCStats"

// SnapshotGCStats delivers are the stats for snapshot GC.
type SnapshotGCStats struct {
	UnreferencedContentCount       uint64 `json:"unreferencedContentCount"`
	UnreferencedContentSize        uint64 `json:"unreferencedContentSize"`
	DeletedContentCount            uint64 `json:"deletedContentCount"`
	DeletedContentSize             uint64 `json:"deletedContentSize"`
	UnreferencedRecentContentCount uint64 `json:"unreferencedRecentContentCount"`
	UnreferencedRecentContentSize  uint64 `json:"unreferencedRecentContentSize"`
	InUseContentCount              uint64 `json:"inUseContentCount"`
	InUseContentSize               uint64 `json:"inUseContentSize"`
	InUseSystemContentCount        uint64 `json:"inUseSystemContentCount"`
	InUseSystemContentSize         uint64 `json:"inUseSystemContentSize"`
	RecoveredContentCount          uint64 `json:"recoveredContentCount"`
	RecoveredContentSize           uint64 `json:"recoveredContentSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (ss *SnapshotGCStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ss.Kind())
	jw.UInt64Field("unreferencedContentCount", ss.UnreferencedContentCount)
	jw.UInt64Field("unreferencedContentSize", ss.UnreferencedContentSize)
	jw.UInt64Field("deletedContentCount", ss.DeletedContentCount)
	jw.UInt64Field("deletedContentSize", ss.DeletedContentSize)
	jw.UInt64Field("unreferencedRecentContentCount", ss.UnreferencedRecentContentCount)
	jw.UInt64Field("unreferencedRecentContentSize", ss.UnreferencedRecentContentSize)
	jw.UInt64Field("inUseContentCount", ss.InUseContentCount)
	jw.UInt64Field("inUseContentSize", ss.InUseContentSize)
	jw.UInt64Field("inUseSystemContentCount", ss.InUseSystemContentCount)
	jw.UInt64Field("inUseSystemContentSize", ss.InUseSystemContentSize)
	jw.UInt64Field("recoveredContentCount", ss.RecoveredContentCount)
	jw.UInt64Field("recoveredContentSize", ss.RecoveredContentSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (ss *SnapshotGCStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unreferenced contents and marked %v(%v) for deletion. Found %v(%v) in-use contents and %v(%v) in-use system contents. Retained %v(%v) recent contents. Recovered %v(%v) contents",
		ss.UnreferencedContentCount, ss.UnreferencedContentSize, ss.DeletedContentCount, ss.DeletedContentSize, ss.InUseContentCount, ss.InUseContentSize,
		ss.InUseSystemContentCount, ss.InUseSystemContentSize, ss.UnreferencedRecentContentCount, ss.UnreferencedRecentContentSize, ss.RecoveredContentCount, ss.RecoveredContentSize)
}

// Kind returns the kind name for the stats.
func (ss *SnapshotGCStats) Kind() string {
	return snapshotGCStatsKind
}
