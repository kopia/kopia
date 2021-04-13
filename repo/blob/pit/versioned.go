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

// VersionMetadataCallback is called when processing the metadata for each blob version.
type VersionMetadataCallback func(VersionMetadata) error

// VersionedStorage defines the API for accessing stores with object versioning enabled.
type VersionedStorage interface {
	// IsVersioned returns whether the backend storage is versioned. Often, this
	// is a property of the blob store bucket.
	IsVersioned(context.Context) (bool, error)

	// GetBlobVersions lists all the versions for the blob with the given ID.
	GetBlobVersions(context.Context, blob.ID, VersionMetadataCallback) error

	// ListBlobVersions lists all versions for all the blobs with the given blob ID prefix.
	ListBlobVersions(context.Context, blob.ID, VersionMetadataCallback) error
}
