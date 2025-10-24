package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const cleanupMarkersStatsKind = "cleanupMarkersStats"

// CleanupMarkersStats are the stats for cleaning up markers.
type CleanupMarkersStats struct {
	DeletedEpochMarkerBlobCount       int `json:"deletedEpochMarkerBlobCount"`
	DeletedDeletionWaterMarkBlobCount int `json:"deletedDeletionWaterMarkBlobCount"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CleanupMarkersStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.IntField("deletedEpochMarkerBlobCount", cs.DeletedEpochMarkerBlobCount)
	jw.IntField("deletedDeletionWaterMarkBlobCount", cs.DeletedDeletionWaterMarkBlobCount)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CleanupMarkersStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v epoch markers and %v deletion watermarks", cs.DeletedEpochMarkerBlobCount, cs.DeletedDeletionWaterMarkBlobCount)
}

// Kind returns the kind name for the stats.
func (cs *CleanupMarkersStats) Kind() string {
	return cleanupMarkersStatsKind
}
