package azure

import (
	"context"

	azblobmodels "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/kopia/kopia/repo/blob"
)

// versionMetadata has metadata for a single BLOB version.
type versionMetadata struct {
	blob.Metadata

	// Version has the format of time.RFC3339Nano
	Version        string
	IsDeleteMarker bool
}

type versionMetadataCallback func(versionMetadata) error

func (az *azPointInTimeStorage) getVersionedBlobMeta(it *azblobmodels.BlobItem) versionMetadata {
	bm := az.getBlobMeta(it)

	return versionMetadata{
		Metadata:       bm,
		Version:        *it.VersionID,
		IsDeleteMarker: az.isAzureDeleteMarker(it),
	}
}

// getBlobVersions lists all the versions for the blob with the given prefix.
func (az *azPointInTimeStorage) getBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	var foundBlobs bool

	if err := az.listBlobVersions(ctx, prefix, func(vm versionMetadata) error {
		foundBlobs = true

		return callback(vm)
	}); err != nil {
		return err
	}

	if !foundBlobs {
		return blob.ErrBlobNotFound
	}

	return nil
}
