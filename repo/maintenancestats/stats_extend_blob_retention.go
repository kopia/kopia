package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const extendBlobRetentionStatsKind = "extendBlobRetentionStats"

// ExtendBlobRetentionStats are the stats for extending blob retention time.
type ExtendBlobRetentionStats struct {
	BlobsToExtend   uint32 `json:"blobsToExtend"`
	BlobsExtended   uint32 `json:"blobsExtended"`
	RetentionPeriod string `json:"retentionPeriod"`
}

// WriteValueTo writes the stats to JSONWriter.
func (es *ExtendBlobRetentionStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(es.Kind())
	jw.UInt32Field("blobsToExtend", es.BlobsToExtend)
	jw.UInt32Field("blobsExtended", es.BlobsExtended)
	jw.StringField("retentionPeriod", es.RetentionPeriod)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (es *ExtendBlobRetentionStats) Summary() string {
	return fmt.Sprintf("Blob retention extension found %v blobs and extended for %v blobs, retention period %v", es.BlobsToExtend, es.BlobsExtended, es.RetentionPeriod)
}

// Kind returns the kind name for the stats.
func (es *ExtendBlobRetentionStats) Kind() string {
	return extendBlobRetentionStatsKind
}
