package snapshot

import (
	"time"

	"github.com/kopia/kopia/object"
)

// Manifest represents information about a single point-in-time filesystem snapshot.
type Manifest struct {
	ID     string     `json:"-"`
	Source SourceInfo `json:"source"`

	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`

	RootObjectID        object.ID `json:"root"`
	HashCacheID         object.ID `json:"hashCache"`
	HashCacheCutoffTime time.Time `json:"hashCacheCutoff"`

	Stats Stats `json:"stats"`

	IncompleteReason string `json:"incomplete,omitempty"`
}
