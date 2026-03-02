package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const compactSingleEpochStatsKind = "compactSingleEpochStats"

// CompactSingleEpochStats are the stats for compacting an index epoch.
type CompactSingleEpochStats struct {
	SupersededIndexBlobCount int   `json:"supersededIndexBlobCount"`
	SupersededIndexTotalSize int64 `json:"supersededIndexTotalSize"`
	Epoch                    int   `json:"epoch"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CompactSingleEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.IntField("supersededIndexBlobCount", cs.SupersededIndexBlobCount)
	jw.Int64Field("supersededIndexTotalSize", cs.SupersededIndexTotalSize)
	jw.IntField("epoch", cs.Epoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CompactSingleEpochStats) Summary() string {
	return fmt.Sprintf("Compacted %v(%v) index blobs for epoch %v", cs.SupersededIndexBlobCount, units.BytesString(cs.SupersededIndexTotalSize), cs.Epoch)
}

// Kind returns the kind name for the stats.
func (cs *CompactSingleEpochStats) Kind() string {
	return compactSingleEpochStatsKind
}
