// Package maintenancestats manages statistics for maintenance tasks.
package maintenancestats

import (
	"encoding/json"

	"github.com/pkg/errors"
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
	case cleanupSupersededIndexesStatsKind:
		result = &CleanupSupersededIndexesStats{}
	case generateRangeCheckpointStatsKind:
		result = &GenerateRangeCheckpointStats{}
	case advanceEpochStatsKind:
		result = &AdvanceEpochStats{}
	default:
		return nil, errors.Wrapf(ErrUnSupportedStatKindError, "invalid kind for stats %v", stats)
	}

	if err := json.Unmarshal(stats.Data, result); err != nil {
		return nil, errors.Wrapf(err, "error unmarshaling raw stats %v of kind %s to %T", stats.Data, stats.Kind, result)
	}

	return result, nil
}
