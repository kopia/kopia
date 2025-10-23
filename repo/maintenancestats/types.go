// Package maintenancestats manages statistics for maintenance tasks.
package maintenancestats

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/contentlog"
)

// Extra holds the data for a maintenance statistics.
type Extra struct {
	Kind string          `json:"kind,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Summarizer defines the methods for summarizing a maintenance statistics.
type Summarizer interface {
	Summary() string
}

// Kind defines the methods for detecting kind of a maintenance statistics.
type Kind interface {
	Kind() string
}

// ErrUnSupportedStatKindError is reported for unsupported stats kind.
var ErrUnSupportedStatKindError = errors.New("unsupported stats kind")

// BuildExtra builds an Extra from maintenance statistics.
func BuildExtra(stats Kind) (Extra, error) {
	if stats == nil {
		return Extra{}, errors.New("invalid stats")
	}

	bytes, err := json.Marshal(stats)
	if err != nil {
		return Extra{}, errors.Wrapf(err, "error marshaling stats %v", stats)
	}

	return Extra{
		Kind: stats.Kind(),
		Data: bytes,
	}, nil
}

// BuildFromExtra builds maintenance statistics from an Extra and returns a Summarizer.
func BuildFromExtra(stats Extra) (Summarizer, error) {
	var result Summarizer

	switch stats.Kind {
	case cleanupMarkersStatsKind:
		result = &CleanupMarkersStats{}
	default:
		return nil, errors.Wrapf(ErrUnSupportedStatKindError, "invalid kind for stats %v", stats)
	}

	if err := json.Unmarshal(stats.Data, result); err != nil {
		return nil, errors.Wrapf(err, "error unmarshaling raw stats %v", stats.Data)
	}

	return result, nil
}

const cleanupMarkersStatsKind = "cleanupMarkersStats"

// CleanupMarkersStats are the stats for cleaning up markers.
type CleanupMarkersStats struct {
	DeletedEpochMarkerBlobs       int `json:"deletedEpochMarkerBlobs"`
	DeletedDeletionWaterMarkBlobs int `json:"deletedDeletionWaterMarkBlobs"`
}

// WriteValueTo writes the stats to JSONWriter.
func (cs *CleanupMarkersStats) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BeginObjectField(cs.Kind())
	jw.IntField("deletedEpochMarkerBlobs", cs.DeletedEpochMarkerBlobs)
	jw.IntField("deletedDeletionWaterMarkBlobs", cs.DeletedDeletionWaterMarkBlobs)
	jw.EndObject()
}

// Summary generates a human readable summary for the stats.
func (cs *CleanupMarkersStats) Summary() string {
	return fmt.Sprintf("Cleaned up %v epoch markers and %v deletion watermarks", cs.DeletedEpochMarkerBlobs, cs.DeletedDeletionWaterMarkBlobs)
}

// Kind returns the kind name for the stats.
func (cs *CleanupMarkersStats) Kind() string {
	return cleanupMarkersStatsKind
}
