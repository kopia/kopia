package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const extendBlobRetentionStatsKind = "extendBlobRetentionStats"

// ExtendBlobRetentionStats are the stats for extending blob retention time.
type ExtendBlobRetentionStats struct {
	ToExtendBlobCount uint64 `json:"toExtendBlobCount"`
	ExtendedBlobCount uint64 `json:"extendedBlobCount"`
	// SkippedReclaimableBlobCount counts pack blobs whose retention was
	// deliberately not extended because garbage collection is about to
	// reclaim them. Reported so an operator can see that the repository is
	// still able to shrink instead of only ever growing.
	SkippedReclaimableBlobCount uint64 `json:"skippedReclaimableBlobCount"`
	RetentionPeriod             string `json:"retentionPeriod"`
}

// WriteValueTo writes the stats to JSONWriter.
func (es *ExtendBlobRetentionStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(es.Kind())
	jw.UInt64Field("toExtendBlobCount", es.ToExtendBlobCount)
	jw.UInt64Field("extendedBlobCount", es.ExtendedBlobCount)
	jw.UInt64Field("skippedReclaimableBlobCount", es.SkippedReclaimableBlobCount)
	jw.StringField("retentionPeriod", es.RetentionPeriod)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (es *ExtendBlobRetentionStats) Summary() string {
	return fmt.Sprintf("Blob retention extension found %v blobs and extended for %v blobs, skipped %v reclaimable blobs, retention period %v",
		es.ToExtendBlobCount, es.ExtendedBlobCount, es.SkippedReclaimableBlobCount, es.RetentionPeriod)
}

// Kind returns the kind name for the stats.
func (es *ExtendBlobRetentionStats) Kind() string {
	return extendBlobRetentionStatsKind
}
