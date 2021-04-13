package pit

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

// VersionMetadata has metadata for a single BLOB version.
type VersionMetadata struct {
	blob.Metadata

	// Versioning related information
	IsLatest       bool
	IsDeleteMarker bool
	Version        string
}

// VersionedStorage defines the API for accessing stores with object versioning
// enabled.
type VersionedStorage interface {
	IsVersioned(ctx context.Context) (bool, error)
}
