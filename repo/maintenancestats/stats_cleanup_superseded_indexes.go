package maintenancestats

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const cleanupSupersededIndexesStatsKind = "cleanupSupersededIndexesStats"

// CleanupSupersededIndexesStats are the stats for cleaning up superseded indexes.
type CleanupSupersededIndexesStats struct {
	MaxReplacementTime time.Time `json:"maxReplacementTime"`
	DeletedBlobCount   int       `json:"deletedBlobCount"`
	DeletedTotalSize   int64     `json:"deletedTotalSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CleanupSupersededIndexesStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.TimeField("maxReplacementTime", cs.MaxReplacementTime)
	jw.IntField("deletedBlobCount", cs.DeletedBlobCount)
	jw.Int64Field("deletedTotalSize", cs.DeletedTotalSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CleanupSupersededIndexesStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v(%v) superseded index blobs, max replacement time %v", cs.DeletedBlobCount, units.BytesString(cs.DeletedTotalSize), cs.MaxReplacementTime)
}

// Kind returns the kind name for the stats.
func (cs *CleanupSupersededIndexesStats) Kind() string {
	return cleanupSupersededIndexesStatsKind
}
