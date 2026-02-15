package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const cleanupLogsStatsKind = "cleanupLogsStats"

// CleanupLogsStats are the stats for cleaning up logs.
type CleanupLogsStats struct {
	ToDeleteBlobCount int   `json:"toDeleteBlobCount"`
	ToDeleteBlobSize  int64 `json:"toDeleteBlobSize"`
	DeletedBlobCount  int   `json:"deletedBlobCount"`
	DeletedBlobSize   int64 `json:"deletedBlobSize"`
	RetainedBlobCount int   `json:"retainedBlobCount"`
	RetainedBlobSize  int64 `json:"retainedBlobSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CleanupLogsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.IntField("toDeleteBlobCount", cs.ToDeleteBlobCount)
	jw.Int64Field("toDeleteBlobSize", cs.ToDeleteBlobSize)
	jw.IntField("deletedBlobCount", cs.DeletedBlobCount)
	jw.Int64Field("deletedBlobSize", cs.DeletedBlobSize)
	jw.IntField("retainedBlobCount", cs.RetainedBlobCount)
	jw.Int64Field("retainedBlobSize", cs.RetainedBlobSize)
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
