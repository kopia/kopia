package gcs

import (
	"context"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"

	"github.com/kopia/kopia/repo/blob"
)

// versionMetadata has metadata for a single BLOB version.
type versionMetadata struct {
	blob.Metadata

	// Versioning related information
	IsLatest       bool
	IsDeleteMarker bool
	Version        string
}

// versionMetadataCallback is called when processing the metadata for each blob version.
type versionMetadataCallback func(versionMetadata) error

// IsVersioned returns whether versioning is enabled in the store.
// It returns true even if versioning is enabled but currently suspended for the
// bucket. Notice that when object locking is enabled in a bucket, object
// versioning is enabled and cannot be suspended.
func (gcs *gcsStorage) IsVersioned(ctx context.Context) (bool, error) {
	attrs, err := gcs.bucket.Attrs(ctx)
	if err != nil {
		return false, errors.Wrapf(err, "could not get versioning info for %s", gcs.BucketName)
	}

	return attrs.VersioningEnabled, nil
}

// getBlobVersions lists all the versions for the blob with the given ID.
func (gcs *gcsStorage) getBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	var foundBlobs bool

	if err := gcs.list(ctx, prefix, true, func(vm versionMetadata) error {
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

// listBlobVersions lists all versions for all the blobs with the given blob ID prefix.
func (gcs *gcsStorage) listBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	return gcs.list(ctx, prefix, false, callback)
}

func (gcs *gcsStorage) list(ctx context.Context, prefix blob.ID, onlyMatching bool, callback versionMetadataCallback) error {

	query := storage.Query{
		Prefix: gcs.getObjectNameString(prefix),
		// Versions true to output all generations of objects
		Versions: true,
	}

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	it := gcs.bucket.Objects(ctx, &query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "could not list objects with prefix %q", query.Prefix)
		}

		if onlyMatching && attrs.Name != query.Prefix {
			return nil
		}

		oi := attrs
		om := infoToVersionMetadata(query.Prefix, oi)

		if errCallback := callback(om); errCallback != nil {
			return errors.Wrapf(errCallback, "callback failed for %q", attrs.Name)
		}
	}

	return nil
}

func toBlobID(blobName, prefix string) blob.ID {
	return blob.ID(strings.TrimPrefix(blobName, prefix))
}

func infoToVersionMetadata(prefix string, oi *storage.ObjectAttrs) versionMetadata {
	bm := blob.Metadata{
		BlobID:    toBlobID(oi.Name, prefix),
		Length:    oi.Size,
		Timestamp: oi.Updated,
	}

	return versionMetadata{
		Metadata:       bm,
		IsLatest:       true, // TODO: how to check if this is the latest version???
		IsDeleteMarker: !oi.Deleted.IsZero(),
		Version:        strconv.FormatInt(oi.Generation, 10),
	}
}
