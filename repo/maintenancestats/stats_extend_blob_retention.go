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
	RetentionPeriod   string `json:"retentionPeriod"`
}

// WriteValueTo writes the stats to JSONWriter.
func (es *ExtendBlobRetentionStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(es.Kind())
	jw.UInt64Field("toExtendBlobCount", es.ToExtendBlobCount)
	jw.UInt64Field("extendedBlobCount", es.ExtendedBlobCount)
	jw.StringField("retentionPeriod", es.RetentionPeriod)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (es *ExtendBlobRetentionStats) Summary() string {
	return fmt.Sprintf("Blob retention extension found %v blobs and extended for %v blobs, retention period %v", es.ToExtendBlobCount, es.ExtendedBlobCount, es.RetentionPeriod)
}

// Kind returns the kind name for the stats.
func (es *ExtendBlobRetentionStats) Kind() string {
	return extendBlobRetentionStatsKind
}
