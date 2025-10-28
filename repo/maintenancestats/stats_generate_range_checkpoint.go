package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
)

const generateRangeCheckpointStatsKind = "generateRangeCheckpointStats"

// GenerateRangeCheckpointStats are the stats for generating range checkpoints.
type GenerateRangeCheckpointStats struct {
	FirstEpoch int `json:"firstEpoch"`
	LastEpoch  int `json:"lastEpoch"`
}

// WriteValueTo writes the stats to JSONWriter.
func (gs *GenerateRangeCheckpointStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(gs.Kind())
	jw.IntField("firstEpoch", gs.FirstEpoch)
	jw.IntField("lastEpoch", gs.LastEpoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (gs *GenerateRangeCheckpointStats) Summary() string {
	return fmt.Sprintf("Generated a range checkpoint from epoch %v to %v", gs.FirstEpoch, gs.LastEpoch)
}

// Kind returns the kind name for the stats.
func (gs *GenerateRangeCheckpointStats) Kind() string {
	return generateRangeCheckpointStatsKind
}
