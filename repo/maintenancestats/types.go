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

// BuildExtra builds an Extra from maintenance statistics.
func BuildExtra(stats Kind) (Extra, error) {
	bytes, err := json.Marshal(stats)
	if err != nil {
		return Extra{}, errors.Wrapf(err, "error marshalling stats %v", stats)
	}

	return Extra{
		Kind: stats.Kind(),
		Data: bytes,
	}, nil
}

// BuildFromExtra builds maintenance statistics from an Extra and returns a Summarizer.
func BuildFromExtra(extra Extra) (Summarizer, error) {
	return nil, nil
}
