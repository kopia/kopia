package maintenancestats

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kopia/kopia/internal/contentlog"
)

// RawStats holds the raw data for maintenance statistics
type RawStats struct {
	Kind string          `json:"kind,omitempty"`
	Raw  json.RawMessage `json:"raw,omitempty"`
}

// Stats defines the methods for maintenance statistics
type Stats interface {
	Kind() string
	Summary() string
}

// BuildRaw builds a kind of Stats into RawStats
func BuildRaw(stats Stats) (RawStats, error) {
	bytes, err := json.Marshal(stats)
	if err != nil {
		return RawStats{}, err
	}

	return RawStats{
		Kind: stats.Kind(),
		Raw:  bytes,
	}, nil
}

// BuildFromRaw a RawStats into Stats
func BuildFromRaw(raw RawStats) (Stats, error) {
	switch raw.Kind {
	case cleanupMarkersStatsKind:
		var cs CleanupMarkersStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case cleanupSupersededIndexesStatsKind:
		var cs CleanupSupersededIndexesStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case generateRangeCheckpointStatsKind:
		var gs GenerateRangeCheckpointStats
		if err := json.Unmarshal(raw.Raw, &gs); err != nil {
			return nil, err
		}

		return &gs, nil

	case advanceEpochStatsKind:
		var as AdvanceEpochStats
		if err := json.Unmarshal(raw.Raw, &as); err != nil {
			return nil, err
		}

		return &as, nil
	case compactSingleEpochStatsKind:
		var cs CompactSingleEpochStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case compactStatsKind:
		var cs CompactStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case deleteUnreferencedPacksStatsKind:
		var ds DeleteUnreferencedPacksStats
		if err := json.Unmarshal(raw.Raw, &ds); err != nil {
			return nil, err
		}

		return &ds, nil
	case extendBlobRetentionStatsKind:
		var es ExtendBlobRetentionStats
		if err := json.Unmarshal(raw.Raw, &es); err != nil {
			return nil, err
		}

		return &es, nil
	case cleanupLogsStatsKind:
		var cs CleanupLogsStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case rewriteContentsStatsKind:
		var rs RewriteContentsStats
		if err := json.Unmarshal(raw.Raw, &rs); err != nil {
			return nil, err
		}

		return &rs, nil
	case snapshotGCStatsKind:
		var ss SnapshotGCStats
		if err := json.Unmarshal(raw.Raw, &ss); err != nil {
			return nil, err
		}

		return &ss, nil
	default:
		return nil, errors.New("unsupported stats kind")
	}
}

const cleanupMarkersStatsKind = "cleanupMarkersStats"

// CleanupMarkersStats are the stats for cleaning up markers
type CleanupMarkersStats struct {
	EpochMarkers       uint32 `json:"epochMarkers"`
	DeletionWaterMarks uint32 `json:"deletionWaterMarks"`
}

// WriteValueTo writes the stats to JSONWriter
func (cs *CleanupMarkersStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.UInt32Field("epochMarkers", cs.EpochMarkers)
	jw.UInt32Field("deletionWaterMarks", cs.DeletionWaterMarks)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (cs *CleanupMarkersStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v epoch markers and %v deletion water marks", cs.EpochMarkers, cs.DeletionWaterMarks)
}

// Kind returns the kind name for CleanupMarkersStats
func (cs *CleanupMarkersStats) Kind() string {
	return cleanupMarkersStatsKind
}

const cleanupSupersededIndexesStatsKind = "cleanupSupersededIndexesStats"

// CleanupSupersededIndexesStats are the stats for Cleaning up superseded indexes
type CleanupSupersededIndexesStats struct {
	MaxReplacementTime time.Time `json:"maxReplacementTime"`
	DeletedBlobCount   uint32    `json:"deletedBlobCount"`
	DeletedTotalSize   int64     `json:"deletedTotalSize"`
}

// WriteValueTo writes the stats to JSONWriter
func (cs *CleanupSupersededIndexesStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.TimeField("maxReplacementTime", cs.MaxReplacementTime)
	jw.UInt32Field("deletedBlobCount", cs.DeletedBlobCount)
	jw.Int64Field("deletedTotalSize", cs.DeletedTotalSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (cs *CleanupSupersededIndexesStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v(%v) superseded index blobs", cs.DeletedBlobCount, cs.DeletedTotalSize)
}

// Kind returns the kind name for CleanupSupersededIndexesStats
func (cs *CleanupSupersededIndexesStats) Kind() string {
	return cleanupSupersededIndexesStatsKind
}

const generateRangeCheckpointStatsKind = "generateRangeCheckpointStats"

// GenerateRangeCheckpointStats are the stats for generating range checkpoints
type GenerateRangeCheckpointStats struct {
	FirstEpoch uint32 `json:"firstEpoch"`
	LastEpoch  uint32 `json:"lastEpoch"`
}

// WriteValueTo writes the stats to JSONWriter
func (gs *GenerateRangeCheckpointStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(gs.Kind())
	jw.UInt32Field("firstEpoch", gs.FirstEpoch)
	jw.UInt32Field("lastEpoch", gs.LastEpoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (gs *GenerateRangeCheckpointStats) Summary() string {
	return fmt.Sprintf("Generated a range checkpoint from epoch %v to %v", gs.FirstEpoch, gs.LastEpoch)
}

// Kind returns the kind name for GenerateRangeCheckpointStats
func (gs *GenerateRangeCheckpointStats) Kind() string {
	return generateRangeCheckpointStatsKind
}

const advanceEpochStatsKind = "advanceEpochStats"

// AdvanceEpochStats are the stats for advancing write epoch
type AdvanceEpochStats struct {
	CurrentEpoch uint32 `json:"currentEpoch"`
	Advanced     bool   `json:"advanced"`
}

// WriteValueTo writes the stats to JSONWriter
func (as *AdvanceEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(as.Kind())
	jw.UInt32Field("currentEpoch", as.CurrentEpoch)
	jw.BoolField("advanced", as.Advanced)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (as *AdvanceEpochStats) Summary() string {
	var message string
	if as.Advanced {
		message = fmt.Sprintf("Advanced epoch to %v", as.CurrentEpoch+1)
	} else {
		message = fmt.Sprintf("Stay at epoch %v", as.CurrentEpoch)
	}

	return message
}

// Kind returns the kind name for AdvanceEpochStats
func (as *AdvanceEpochStats) Kind() string {
	return advanceEpochStatsKind
}

const compactSingleEpochStatsKind = "compactSingleEpochStats"

// CompactSingleEpochStats are the stats for compacting an index epoch
type CompactSingleEpochStats struct {
	CompactedBlobCount uint32 `json:"compactedBlobCount"`
	CompactedBlobSize  int64  `json:"compactedBlobSize"`
	Epoch              uint32 `json:"epoch"`
}

// WriteValueTo writes the stats to JSONWriter
func (cs *CompactSingleEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.UInt32Field("compactedBlobCount", cs.CompactedBlobCount)
	jw.Int64Field("compactedBlobSize", cs.CompactedBlobSize)
	jw.UInt32Field("epoch", cs.Epoch)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (cs *CompactSingleEpochStats) Summary() string {
	return fmt.Sprintf("Compacted %v(%v) index blobs for epoch %v", cs.CompactedBlobCount, cs.CompactedBlobSize, cs.Epoch)
}

// Kind returns the kind name for CompactSingleEpochStats
func (cs *CompactSingleEpochStats) Kind() string {
	return compactSingleEpochStatsKind
}

const compactStatsKind = "compactStats"

// CompactStats are the stats for compacting indexes
type CompactStats struct {
	DroppedBefore time.Time `json:"droppedBefore"`
}

// WriteValueTo writes the stats to JSONWriter
func (cs *CompactStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.TimeField("droppedBefore", cs.DroppedBefore)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (cs *CompactStats) Summary() string {
	return fmt.Sprintf("Dropped indexes before %v", cs.DroppedBefore)
}

// Kind returns the kind name for CompactStats
func (cs *CompactStats) Kind() string {
	return compactStatsKind
}

const deleteUnreferencedPacksStatsKind = "deleteUnreferencedPacksStats"

// DeleteUnreferencedPacksStats are the stats for deleting unreferenced packs
type DeleteUnreferencedPacksStats struct {
	UnusedCount   uint32 `json:"unusedCount"`
	UnusedSize    int64  `json:"unusedSize"`
	DeletedCount  uint32 `json:"deletedCount"`
	DeletedSize   int64  `json:"deletedSize"`
	RetainedCount uint32 `json:"retainedCount"`
	RetainedSize  int64  `json:"retainedSize"`
}

// WriteValueTo writes the stats to JSONWriter
func (ds *DeleteUnreferencedPacksStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ds.Kind())
	jw.UInt32Field("unusedCount", uint32(ds.UnusedCount))
	jw.Int64Field("unusedSize", ds.UnusedSize)
	jw.UInt32Field("deletedCount", uint32(ds.DeletedCount))
	jw.Int64Field("deletedSize", ds.DeletedSize)
	jw.UInt32Field("retainedCount", uint32(ds.RetainedCount))
	jw.Int64Field("retainedSize", ds.RetainedSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (ds *DeleteUnreferencedPacksStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unreferenced blobs, deleted %v(%v) and retained %v(%v).", ds.UnusedCount, ds.UnusedSize, ds.DeletedCount, ds.DeletedSize, ds.RetainedCount, ds.RetainedSize)
}

// Kind returns the kind name for DeleteUnreferencedPacksStats
func (ds *DeleteUnreferencedPacksStats) Kind() string {
	return deleteUnreferencedPacksStatsKind
}

const extendBlobRetentionStatsKind = "extendBlobRetentionStats"

// ExtendBlobRetentionStats are the stats for extending blob retention time
type ExtendBlobRetentionStats struct {
	ToExtend uint32 `json:"toExtend"`
	Extended uint32 `json:"extended"`
}

// WriteValueTo writes the stats to JSONWriter
func (es *ExtendBlobRetentionStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(es.Kind())
	jw.UInt32Field("toExtend", es.ToExtend)
	jw.UInt32Field("extended", es.Extended)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (es *ExtendBlobRetentionStats) Summary() string {
	return fmt.Sprintf("Found %v blobs for retention time extent and extended %v of them", es.ToExtend, es.Extended)
}

// Kind returns the kind name for DeleteUnreferencedBlobsStats
func (es *ExtendBlobRetentionStats) Kind() string {
	return extendBlobRetentionStatsKind
}

const cleanupLogsStatsKind = "cleanupLogsStats"

// CleanupLogsStats are the stats for cleanning up logs
type CleanupLogsStats struct {
	UnusedCount   uint32 `json:"unusedCount"`
	UnusedSize    int64  `json:"unusedSize"`
	RetainedCount uint32 `json:"retainedCount"`
	RetainedSize  int64  `json:"retainedSize"`
}

// WriteValueTo writes the stats to JSONWriter
func (cs *CleanupLogsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.UInt32Field("unusedCount", cs.UnusedCount)
	jw.Int64Field("unusedSize", cs.UnusedSize)
	jw.UInt32Field("retainedCount", cs.RetainedCount)
	jw.Int64Field("retainedSize", cs.RetainedSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (cs *CleanupLogsStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v(%v) logs blobs, retained %v(%v) logs blobs.", cs.UnusedCount, cs.UnusedSize, cs.RetainedCount, cs.RetainedSize)
}

// Kind returns the kind name for CleanupLogsStats
func (cs *CleanupLogsStats) Kind() string {
	return cleanupLogsStatsKind
}

const rewriteContentsStatsKind = "rewriteContentsStats"

// RewriteContentsStats are the stats for rewritting contents
type RewriteContentsStats struct {
	RewrittenCount uint32 `json:"rewrittenCount"`
	RewrittenSize  int64  `json:"rewrittenSize"`
	RetainedCount  uint32 `json:"retainedCount"`
	RetainedSize   int64  `json:"retainedSize"`
}

// WriteValueTo writes the stats to JSONWriter
func (rs *RewriteContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(rs.Kind())
	jw.UInt32Field("rewrittenCount", rs.RewrittenCount)
	jw.Int64Field("rewrittenSize", rs.RewrittenSize)
	jw.UInt32Field("retainedCount", rs.RetainedCount)
	jw.Int64Field("retainedSize", rs.RetainedSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (rs *RewriteContentsStats) Summary() string {
	return fmt.Sprintf("Rewritten %v(%v) contents, retained %v(%v) contents", rs.RewrittenCount, rs.RewrittenSize, rs.RetainedCount, rs.RetainedSize)
}

// Kind returns the kind name for RewriteContentsStats
func (rs *RewriteContentsStats) Kind() string {
	return rewriteContentsStatsKind
}

const snapshotGCStatsKind = "snapshotGCStats"

// SnapshotGCStats delivers are the stats for snapshot GC
type SnapshotGCStats struct {
	UnusedCount          uint32 `json:"unusedCount"`
	UnusedSize           int64  `json:"unusedSize"`
	TooRecentUnusedCount uint32 `json:"tooRecentUnusedCount"`
	TooRecentUnusedSize  int64  `json:"tooRecentUnusedSize"`
	InUseCount           uint32 `json:"inUseCount"`
	IntUseSize           int64  `json:"intUseSize"`
	InUseSystemCount     uint32 `json:"inUseSystemCount"`
	InUseSystemSize      int64  `json:"inUseSystemSize"`
	RecoveredCount       uint32 `json:"recoveredCount"`
	RecoveredSize        int64  `json:"recoveredSize"`
}

// WriteValueTo writes the stats to JSONWriter
func (ss *SnapshotGCStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ss.Kind())
	jw.UInt32Field("unusedCount", ss.UnusedCount)
	jw.Int64Field("unusedSize", ss.UnusedSize)
	jw.UInt32Field("tooRecentUnusedCount", ss.TooRecentUnusedCount)
	jw.Int64Field("tooRecentUnusedSize", ss.TooRecentUnusedSize)
	jw.UInt32Field("inUseCount", ss.InUseCount)
	jw.Int64Field("intUseSize", ss.IntUseSize)
	jw.UInt32Field("inUseSystemCount", ss.InUseSystemCount)
	jw.Int64Field("inUseSystemSize", ss.InUseSystemSize)
	jw.UInt32Field("recoveredCount", ss.RecoveredCount)
	jw.Int64Field("recoveredSize", ss.RecoveredSize)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats
func (ss *SnapshotGCStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unused contents, %v(%v) inused contents, %v(%v) inused system contents, marked %v(%v) unused countents for deletion, recovered %v(%v) contents",
		ss.UnusedCount+ss.TooRecentUnusedCount, ss.UnusedSize+ss.TooRecentUnusedSize, ss.InUseCount, ss.IntUseSize, ss.InUseSystemCount, ss.InUseSystemSize, ss.UnusedCount, ss.UnusedSize, ss.RecoveredCount, ss.RecoveredSize)
}

// Kind returns the kind name for SnapshotGCStats
func (ss *SnapshotGCStats) Kind() string {
	return snapshotGCStatsKind
}
