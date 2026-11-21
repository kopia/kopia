package maintenancestats

import (
	"fmt"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/units"
)

const consolidateRangeCheckpointsStatsKind = "consolidateRangeCheckpointsStats"

// ConsolidateRangeCheckpointsStats are the stats for consolidating the range checkpoint chain into one checkpoint.
type ConsolidateRangeCheckpointsStats struct {
	RangeMinEpoch         uint64 `json:"rangeMinEpoch"`
	RangeMaxEpoch         uint64 `json:"rangeMaxEpoch"`
	InputRangeCheckpoints uint64 `json:"inputRangeCheckpoints"`
	InputBlobCount        uint64 `json:"inputBlobCount"`
	InputTotalSize        uint64 `json:"inputTotalSize"`
	InputEntries          uint64 `json:"inputEntries"`
	DroppedDeletedEntries uint64 `json:"droppedDeletedEntries"`
	OutputEntries         uint64 `json:"outputEntries"`
	OutputBlobCount       uint64 `json:"outputBlobCount"`
	OutputTotalSize       uint64 `json:"outputTotalSize"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *ConsolidateRangeCheckpointsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.UInt64Field("rangeMinEpoch", cs.RangeMinEpoch)
	jw.UInt64Field("rangeMaxEpoch", cs.RangeMaxEpoch)
	jw.UInt64Field("inputRangeCheckpoints", cs.InputRangeCheckpoints)
	jw.UInt64Field("inputBlobCount", cs.InputBlobCount)
	jw.UInt64Field("inputTotalSize", cs.InputTotalSize)
	jw.UInt64Field("inputEntries", cs.InputEntries)
	jw.UInt64Field("droppedDeletedEntries", cs.DroppedDeletedEntries)
	jw.UInt64Field("outputEntries", cs.OutputEntries)
	jw.UInt64Field("outputBlobCount", cs.OutputBlobCount)
	jw.UInt64Field("outputTotalSize", cs.OutputTotalSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *ConsolidateRangeCheckpointsStats) Summary() string {
	return fmt.Sprintf("Consolidated %v range checkpoints (%v blobs, %v, %v entries) into one checkpoint for epochs %v to %v (%v blobs, %v, %v entries), dropped %v deleted entries",
		cs.InputRangeCheckpoints, cs.InputBlobCount, units.BytesString(cs.InputTotalSize), cs.InputEntries,
		cs.RangeMinEpoch, cs.RangeMaxEpoch, cs.OutputBlobCount, units.BytesString(cs.OutputTotalSize), cs.OutputEntries,
		cs.DroppedDeletedEntries)
}

// Kind returns the kind name for the stats.
func (cs *ConsolidateRangeCheckpointsStats) Kind() string {
	return consolidateRangeCheckpointsStatsKind
}
