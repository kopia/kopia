package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const deleteUnreferencedPacksStatsKind = "deleteUnreferencedPacksStats"

// DeleteUnreferencedPacksStats are the stats for deleting unreferenced packs.
type DeleteUnreferencedPacksStats struct {
	UnreferencedPackCount uint32 `json:"unreferencedPackCount"`
	UnreferencedTotalSize int64  `json:"unreferencedTotalSize"`
	DeletedPackCount      uint32 `json:"deletedPackCount"`
	DeletedTotalSize      int64  `json:"deletedTotalSize"`
	RetainedPackCount     uint32 `json:"retainedPackCount"`
	RetainedTotalSize     int64  `json:"retainedTotalSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (ds *DeleteUnreferencedPacksStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ds.Kind())
	jw.UInt32Field("unreferencedPackCount", ds.UnreferencedPackCount)
	jw.Int64Field("unreferencedTotalSize", ds.UnreferencedTotalSize)
	jw.UInt32Field("deletedPackCount", ds.DeletedPackCount)
	jw.Int64Field("deletedTotalSize", ds.DeletedTotalSize)
	jw.UInt32Field("retainedPackCount", ds.RetainedPackCount)
	jw.Int64Field("retainedTotalSize", ds.RetainedTotalSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (ds *DeleteUnreferencedPacksStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unreferenced pack blobs to delete and deleted %v(%v). Retained %v(%v) unreferenced pack blobs.",
		ds.UnreferencedPackCount, units.BytesString(ds.UnreferencedTotalSize), ds.DeletedPackCount, units.BytesString(ds.DeletedTotalSize), ds.RetainedPackCount, units.BytesString(ds.RetainedTotalSize))
}

// Kind returns the kind name for the stats.
func (ds *DeleteUnreferencedPacksStats) Kind() string {
	return deleteUnreferencedPacksStatsKind
}
