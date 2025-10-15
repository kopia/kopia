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
	Type string          `json:"type,omitempty"`
	Raw  json.RawMessage `json:"raw,omitempty"`
}

// Stats defines the methods for maintenance statistics
type Stats interface {
	Type() string
	Summary() string
}

// BuildRaw builds a type of Stats into RawStats
func BuildRaw(stats Stats) (RawStats, error) {
	bytes, err := json.Marshal(stats)
	if err != nil {
		return RawStats{}, err
	}

	return RawStats{
		Type: stats.Type(),
		Raw:  bytes,
	}, nil
}

// BuildFromRaw a RawStats into Stats
func BuildFromRaw(raw RawStats) (Stats, error) {
	switch raw.Type {
	case cleanupMarkersStatsType:
		var cs CleanupMarkersStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case cleanupSupersededIndexesStatsType:
		var cs CleanupSupersededIndexesStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case generateRangeCheckpointStatsType:
		var gs GenerateRangeCheckpointStats
		if err := json.Unmarshal(raw.Raw, &gs); err != nil {
			return nil, err
		}

		return &gs, nil

	case advanceEpochStatsType:
		var as AdvanceEpochStats
		if err := json.Unmarshal(raw.Raw, &as); err != nil {
			return nil, err
		}

		return &as, nil
	case compactSingleEpochStatsType:
		var cs CompactSingleEpochStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case compactStatsType:
		var cs CompactStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case deleteUnreferencedBlobsStatsType:
		var ds DeleteUnreferencedBlobsStats
		if err := json.Unmarshal(raw.Raw, &ds); err != nil {
			return nil, err
		}

		return &ds, nil
	case extendBlobRetentionStatsType:
		var es ExtendBlobRetentionStats
		if err := json.Unmarshal(raw.Raw, &es); err != nil {
			return nil, err
		}

		return &es, nil
	case cleanupLogsStatsType:
		var cs CleanupLogsStats
		if err := json.Unmarshal(raw.Raw, &cs); err != nil {
			return nil, err
		}

		return &cs, nil
	case rewriteContentsStatsType:
		var rs RewriteContentsStats
		if err := json.Unmarshal(raw.Raw, &rs); err != nil {
			return nil, err
		}

		return &rs, nil
	case snapshotGCStatsType:
		var ss SnapshotGCStats
		if err := json.Unmarshal(raw.Raw, &ss); err != nil {
			return nil, err
		}

		return &ss, nil
	default:
		return nil, errors.New("unsupported stats type")
	}
}

const cleanupMarkersStatsType = "cleanupMarkersStats"

// CleanupMarkersStats delivers the statistics for CleanupMarkers
type CleanupMarkersStats struct {
	EpochMarkers       uint32 `json:"epochMarkers"`
	DeletionWaterMarks uint32 `json:"deletionWaterMarks"`
}

// WriteValueTo writes CleanupMarkersStats to JSONWriter
func (cs *CleanupMarkersStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Type())
	jw.UInt32Field("epochMarkers", cs.EpochMarkers)
	jw.UInt32Field("deletionWaterMarks", cs.DeletionWaterMarks)
	jw.EndObject()
}

// Summary generates readable summary for CleanupMarkersStats
func (cs *CleanupMarkersStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v epoch markers and %v deletion water marks", cs.EpochMarkers, cs.DeletionWaterMarks)
}

// Type returns the type name for CleanupMarkersStats
func (cs *CleanupMarkersStats) Type() string {
	return cleanupMarkersStatsType
}

const cleanupSupersededIndexesStatsType = "cleanupSupersededIndexesStats"

// CleanupSupersededIndexesStats delivers the statistics for CleanupSupersededIndexes
type CleanupSupersededIndexesStats struct {
	MaxReplacementTime time.Time `json:"maxReplacementTime"`
	DeletedBlobs       uint32    `json:"deletedBlobs"`
	DeletedSize        int64     `json:"deletedSize"`
}

// WriteValueTo writes CleanupSupersededIndexesStats to JSONWriter
func (cs *CleanupSupersededIndexesStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Type())
	jw.TimeField("maxReplacementTime", cs.MaxReplacementTime)
	jw.UInt32Field("deletedBlobs", cs.DeletedBlobs)
	jw.Int64Field("deletedSize", cs.DeletedSize)
	jw.EndObject()
}

// Summary generates readable summary for CleanupSupersededIndexesStats
func (cs *CleanupSupersededIndexesStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v(%v) superseded index blobs", cs.DeletedBlobs, cs.DeletedSize)
}

// Type returns the type name for CleanupSupersededIndexesStats
func (cs *CleanupSupersededIndexesStats) Type() string {
	return cleanupSupersededIndexesStatsType
}

const generateRangeCheckpointStatsType = "generateRangeCheckpointStats"

// GenerateRangeCheckpointStats delivers the statistics for MaybeGenerateRangeCheckpoint
type GenerateRangeCheckpointStats struct {
	FirstEpoch uint32 `json:"firstEpoch"`
	LastEpoch  uint32 `json:"lastEpoch"`
}

// WriteValueTo writes GenerateRangeCheckpointStats to JSONWriter
func (gs *GenerateRangeCheckpointStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(gs.Type())
	jw.UInt32Field("firstEpoch", gs.FirstEpoch)
	jw.UInt32Field("lastEpoch", gs.LastEpoch)
	jw.EndObject()
}

// Summary generates readable summary for GenerateRangeCheckpointStats
func (gs *GenerateRangeCheckpointStats) Summary() string {
	return fmt.Sprintf("Generated a range checkpoint from epoch %v to %v", gs.FirstEpoch, gs.LastEpoch)
}

// Type returns the type name for GenerateRangeCheckpointStats
func (gs *GenerateRangeCheckpointStats) Type() string {
	return generateRangeCheckpointStatsType
}

const advanceEpochStatsType = "advanceEpochStats"

// AdvanceEpochStats delivers the statistics for MaybeAdvanceWriteEpoch
type AdvanceEpochStats struct {
	CurEpoch uint32 `json:"curEpoch"`
	Advanced bool   `json:"advanced"`
}

// WriteValueTo writes AdvanceEpochStats to JSONWriter
func (as *AdvanceEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(as.Type())
	jw.UInt32Field("curEpoch", as.CurEpoch)
	jw.BoolField("advanced", as.Advanced)
	jw.EndObject()
}

// Summary generates readable summary for AdvanceEpochStats
func (as *AdvanceEpochStats) Summary() string {
	var message string
	if as.Advanced {
		message = fmt.Sprintf("Advanced epoch to %v", as.CurEpoch+1)
	} else {
		message = fmt.Sprintf("Stay at epoch %v", as.CurEpoch)
	}

	return message
}

// Type returns the type name for AdvanceEpochStats
func (as *AdvanceEpochStats) Type() string {
	return advanceEpochStatsType
}

const compactSingleEpochStatsType = "compactSingleEpochStats"

// CompactSingleEpochStats delivers the statistics for MaybeCompactSingleEpoch
type CompactSingleEpochStats struct {
	BlobCount uint32 `json:"blobCount"`
	BlobSize  int64  `json:"blobSize"`
	Epoch     uint32 `json:"epoch"`
}

// WriteValueTo writes CompactSingleEpochStats to JSONWriter
func (cs *CompactSingleEpochStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Type())
	jw.UInt32Field("blobCount", cs.BlobCount)
	jw.Int64Field("blobSize", cs.BlobSize)
	jw.UInt32Field("epoch", cs.Epoch)
	jw.EndObject()
}

// Summary generates readable summary for CompactSingleEpochStats
func (cs *CompactSingleEpochStats) Summary() string {
	return fmt.Sprintf("Compacted %v(%v) index blobs for epoch %v", cs.BlobCount, cs.BlobSize, cs.Epoch)
}

// Type returns the type name for CompactSingleEpochStats
func (cs *CompactSingleEpochStats) Type() string {
	return compactSingleEpochStatsType
}

const compactStatsType = "compactStats"

// CompactStats delivers the statistics for Compact
type CompactStats struct {
	DroppedBefore time.Time `json:"droppedBefore"`
}

// WriteValueTo writes CompactStats to JSONWriter
func (cs *CompactStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Type())
	jw.TimeField("droppedBefore", cs.DroppedBefore)
	jw.EndObject()
}

// Summary generates readable summary for CompactStats
func (cs *CompactStats) Summary() string {
	return fmt.Sprintf("Dropped indexes before %v", cs.DroppedBefore)
}

// Type returns the type name for CompactStats
func (cs *CompactStats) Type() string {
	return compactStatsType
}

const deleteUnreferencedBlobsStatsType = "deleteUnreferencedBlobsStats"

// DeleteUnreferencedBlobsStats delivers the statistics for DeleteUnreferencedBlobs
type DeleteUnreferencedBlobsStats struct {
	UnusedCount    uint32 `json:"unusedCount"`
	UnusedSize     int64  `json:"unusedSize"`
	DeletedCount   uint32 `json:"deletedCount"`
	DeletedSize    int64  `json:"deletedSize"`
	PreservedCount uint32 `json:"PreservedCount"`
	PreservedSize  int64  `json:"PreservedSize"`
}

// WriteValueTo writes DeleteUnreferencedBlobsStats to JSONWriter
func (ds *DeleteUnreferencedBlobsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ds.Type())
	jw.UInt32Field("unusedCount", uint32(ds.UnusedCount))
	jw.Int64Field("unusedSize", ds.UnusedSize)
	jw.UInt32Field("deletedCount", uint32(ds.DeletedCount))
	jw.Int64Field("deletedSize", ds.DeletedSize)
	jw.UInt32Field("PreservedCount", uint32(ds.PreservedCount))
	jw.Int64Field("PreservedSize", ds.PreservedSize)
	jw.EndObject()
}

// Summary generates readable summary for DeleteUnreferencedBlobsStats
func (ds *DeleteUnreferencedBlobsStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unreferenced blobs, deleted %v(%v) and preserved %v(%v).", ds.UnusedCount, ds.UnusedSize, ds.DeletedCount, ds.DeletedSize, ds.PreservedCount, ds.PreservedSize)
}

// Type returns the type name for DeleteUnreferencedBlobsStats
func (ds *DeleteUnreferencedBlobsStats) Type() string {
	return deleteUnreferencedBlobsStatsType
}

const extendBlobRetentionStatsType = "extendBlobRetentionStats"

// ExtendBlobRetentionStats delivers the statistics for ExtendBlobRetentionTime
type ExtendBlobRetentionStats struct {
	ToExtend uint32 `json:"toExtend"`
	Extended uint32 `json:"extended"`
}

// WriteValueTo writes ExtendBlobRetentionStats to JSONWriter
func (es *ExtendBlobRetentionStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(es.Type())
	jw.UInt32Field("toExtend", es.ToExtend)
	jw.UInt32Field("extended", es.Extended)
	jw.EndObject()
}

// Summary generates readable summary for ExtendBlobRetentionStats
func (es *ExtendBlobRetentionStats) Summary() string {
	return fmt.Sprintf("Found %v blobs for retention time extent and extended %v of them", es.ToExtend, es.Extended)
}

// Type returns the type name for DeleteUnreferencedBlobsStats
func (es *ExtendBlobRetentionStats) Type() string {
	return extendBlobRetentionStatsType
}

const cleanupLogsStatsType = "cleanupLogsStats"

// CleanupLogsStats delivers the statistics for CleanupLogs
type CleanupLogsStats struct {
	UnusedCount    uint32 `json:"unusedCount"`
	UnusedSize     int64  `json:"unusedSize"`
	PreservedCount uint32 `json:"preservedCount"`
	PreservedSize  int64  `json:"preservedSize"`
}

// WriteValueTo writes CleanupLogsStats to JSONWriter
func (cs *CleanupLogsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Type())
	jw.UInt32Field("unusedCount", cs.UnusedCount)
	jw.Int64Field("unusedSize", cs.UnusedSize)
	jw.UInt32Field("preservedCount", cs.PreservedCount)
	jw.Int64Field("preservedSize", cs.PreservedSize)
	jw.EndObject()
}

// Summary generates readable summary for CleanupLogsStats
func (cs *CleanupLogsStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v(%v) logs blobs, preserved %v(%v) logs blobs.", cs.UnusedCount, cs.UnusedSize, cs.PreservedCount, cs.PreservedSize)
}

// Type returns the type name for CleanupLogsStats
func (cs *CleanupLogsStats) Type() string {
	return cleanupLogsStatsType
}

const rewriteContentsStatsType = "rewriteContentsStats"

// RewriteContentsStats delivers the statistics for RewriteContents
type RewriteContentsStats struct {
	RewrittenCount uint32 `json:"rewrittenCount"`
	RewrittenSize  int64  `json:"rewrittenSize"`
	PreservedCount uint32 `json:"preservedCount"`
	PreservedSize  int64  `json:"preservedSize"`
}

// WriteValueTo writes RewriteContentsStats to JSONWriter
func (rs *RewriteContentsStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(rs.Type())
	jw.UInt32Field("rewrittenCount", rs.RewrittenCount)
	jw.Int64Field("rewrittenSize", rs.RewrittenSize)
	jw.UInt32Field("preservedCount", rs.PreservedCount)
	jw.Int64Field("preservedSize", rs.PreservedSize)
	jw.EndObject()
}

// Summary generates readable summary for RewriteContentsStats
func (rs *RewriteContentsStats) Summary() string {
	return fmt.Sprintf("Rewritten %v(%v) contents, preserved %v(%v) contents", rs.RewrittenCount, rs.RewrittenSize, rs.PreservedCount, rs.PreservedSize)
}

// Type returns the type name for RewriteContentsStats
func (rs *RewriteContentsStats) Type() string {
	return rewriteContentsStatsType
}

const snapshotGCStatsType = "snapshotGCStats"

// SnapshotGCStats delivers the statistics for SnapshotGC
type SnapshotGCStats struct {
	UnusedCount          uint32 `json:"unusedCount"`
	UnusedSize           int64  `json:"unusedSize"`
	TooRecentUnusedCount uint32 `json:"tooRecentUnusedCount"`
	TooRecentUnusedSize  int64  `json:"tooRecentUnusedSize"`
	InUseCount           uint32 `json:"inUseCount"`
	IntUseSize           int64  `json:"intUseSize"`
	InUseSystemCount     uint32 `json:"inUseSystemCount"`
	IntUseSystemSize     int64  `json:"intUseSystemSize"`
	RecoveredCount       uint32 `json:"recoveredCount"`
	RecoveredSize        int64  `json:"recoveredSize"`
}

// WriteValueTo writes SnapshotGCStats to JSONWriter
func (ss *SnapshotGCStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(ss.Type())
	jw.UInt32Field("unusedCount", ss.UnusedCount)
	jw.Int64Field("unusedSize", ss.UnusedSize)
	jw.UInt32Field("tooRecentUnusedCount", ss.TooRecentUnusedCount)
	jw.Int64Field("tooRecentUnusedSize", ss.TooRecentUnusedSize)
	jw.UInt32Field("inUseCount", ss.InUseCount)
	jw.Int64Field("intUseSize", ss.IntUseSize)
	jw.UInt32Field("inUseSystemCount", ss.InUseSystemCount)
	jw.Int64Field("intUseSystemSize", ss.IntUseSystemSize)
	jw.UInt32Field("recoveredCount", ss.RecoveredCount)
	jw.Int64Field("recoveredSize", ss.RecoveredSize)
	jw.EndObject()
}

// Summary generates readable summary for SnapshotGCStats
func (ss *SnapshotGCStats) Summary() string {
	return fmt.Sprintf("Found %v(%v) unused contents, %v(%v) inused contents, %v(%v) inused system contents, marked %v(%v) unused countents for deletion, recovered %v(%v) contents",
		ss.UnusedCount+ss.TooRecentUnusedCount, ss.UnusedSize+ss.TooRecentUnusedSize, ss.InUseCount, ss.IntUseSize, ss.InUseSystemCount, ss.IntUseSystemSize, ss.UnusedCount, ss.UnusedSize, ss.RecoveredCount, ss.RecoveredSize)
}

// Type returns the type name for SnapshotGCStats
func (ss *SnapshotGCStats) Type() string {
	return snapshotGCStatsType
}
