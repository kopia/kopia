package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const cleanupLogsStatsKind = "cleanupLogsStats"

// CleanupLogsStats are the stats for cleaning up logs.
type CleanupLogsStats struct {
	ToDeleteBlobCount uint64 `json:"toDeleteBlobCount"`
	ToDeleteBlobSize  uint64 `json:"toDeleteBlobSize"`
	DeletedBlobCount  uint64 `json:"deletedBlobCount"`
	DeletedBlobSize   uint64 `json:"deletedBlobSize"`
	RetainedBlobCount uint64 `json:"retainedBlobCount"`
	RetainedBlobSize  uint64 `json:"retainedBlobSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CleanupLogsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.UInt64Field("toDeleteBlobCount", cs.ToDeleteBlobCount)
	jw.UInt64Field("toDeleteBlobSize", cs.ToDeleteBlobSize)
	jw.UInt64Field("deletedBlobCount", cs.DeletedBlobCount)
	jw.UInt64Field("deletedBlobSize", cs.DeletedBlobSize)
	jw.UInt64Field("retainedBlobCount", cs.RetainedBlobCount)
	jw.UInt64Field("retainedBlobSize", cs.RetainedBlobSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CleanupLogsStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) logs blobs for deletion and deleted %v(%v) of them. Retained %v(%v) log blobs.",
		cs.ToDeleteBlobCount, units.BytesString(cs.ToDeleteBlobSize), cs.DeletedBlobCount,
		units.BytesString(cs.DeletedBlobSize), cs.RetainedBlobCount,
		units.BytesString(cs.RetainedBlobSize))
}

// Kind returns the kind name for the stats.
func (cs *CleanupLogsStats) Kind() string {
	return cleanupLogsStatsKind
}
