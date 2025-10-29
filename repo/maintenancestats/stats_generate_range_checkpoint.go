package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const generateRangeCheckpointStatsKind = "generateRangeCheckpointStats"

// GenerateRangeCheckpointStats are the stats for generating range checkpoints.
type GenerateRangeCheckpointStats struct {
	RangeMinEpoch int `json:"rangeMinEpoch"`
	RangeMaxEpoch int `json:"rangeMaxEpoch"`
}

// WriteValueTo writes the stats to JSONWriter.
func (gs *GenerateRangeCheckpointStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(gs.Kind())
	jw.IntField("rangeMinEpoch", gs.RangeMinEpoch)
	jw.IntField("rangeMaxEpoch", gs.RangeMaxEpoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (gs *GenerateRangeCheckpointStats) Summary() string {
	return fmt.Sprintf("Generated a range checkpoint from epoch %v to %v inclusive", gs.RangeMinEpoch, gs.RangeMaxEpoch)
}

// Kind returns the kind name for the stats.
func (gs *GenerateRangeCheckpointStats) Kind() string {
	return generateRangeCheckpointStatsKind
}
