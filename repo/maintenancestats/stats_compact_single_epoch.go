package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const compactSingleEpochStatsKind = "compactSingleEpochStats"

// CompactSingleEpochStats are the stats for compacting an index epoch.
type CompactSingleEpochStats struct {
	CompactedBlobCount int   `json:"compactedBlobCount"`
	CompactedBlobSize  int64 `json:"compactedBlobSize"`
	Epoch              int   `json:"epoch"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CompactSingleEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.IntField("compactedBlobCount", cs.CompactedBlobCount)
	jw.Int64Field("compactedBlobSize", cs.CompactedBlobSize)
	jw.IntField("epoch", cs.Epoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CompactSingleEpochStats) Summary() string {
	return fmt.Sprintf("Compacted %v(%v) index blobs for epoch %v", cs.CompactedBlobCount, cs.CompactedBlobSize, cs.Epoch)
}

// Kind returns the kind name for the stats.
func (cs *CompactSingleEpochStats) Kind() string {
	return compactSingleEpochStatsKind
}
