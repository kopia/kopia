package s3

import (
	"context"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/pit"
)

// IsVersioned returns whether versioning is enabled in the store.
// It returns true even if versioning is enabled but currently suspended for the
// bucket. Notice that when object locking is enabled in a bucket, object
// versioning is enabled and cannot be suspended.
func (s *s3Storage) IsVersioned(ctx context.Context) (bool, error) {
	vi, err := s.cli.GetBucketVersioning(ctx, s.BucketName)
	if err != nil {
		return false, errors.Wrapf(err, "could not get versioning info for %s", s.BucketName)
	}

	return vi.Enabled(), nil
}

func (s *s3Storage) GetBlobVersions(ctx context.Context, prefix blob.ID, callback pit.VersionMetadataCallback) error {
	var foundBlobs bool

	if err := s.list(ctx, prefix, true, func(vm pit.VersionMetadata) error {
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

func (s *s3Storage) ListBlobVersions(ctx context.Context, prefix blob.ID, callback pit.VersionMetadataCallback) error {
	return s.list(ctx, prefix, false, callback)
}

func (s *s3Storage) list(ctx context.Context, prefix blob.ID, onlyMatching bool, callback pit.VersionMetadataCallback) error {
	opts := minio.ListObjectsOptions{
		Prefix:       s.getObjectNameString(prefix),
		Recursive:    !onlyMatching,
		WithVersions: true,
	}

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	for o := range s.cli.ListObjects(ctx, s.BucketName, opts) {
		if o.Err != nil {
			return errors.Wrapf(o.Err, "could not list objects with prefix %q", opts.Prefix)
		}

		if onlyMatching && o.Key != opts.Prefix {
			return nil
		}

		oi := o
		om := infoToVersionMetadata(s.Prefix, &oi)

		if err := callback(om); err != nil {
			return errors.Wrapf(err, "callback failed for %q", o.Key)
		}
	}

	return nil
}

func toBlobID(blobName, prefix string) blob.ID {
	return blob.ID(strings.TrimPrefix(blobName, prefix))
}

func infoToVersionMetadata(prefix string, oi *minio.ObjectInfo) pit.VersionMetadata {
	return pit.VersionMetadata{
		Metadata: blob.Metadata{
			BlobID:    toBlobID(oi.Key, prefix),
			Length:    oi.Size,
			Timestamp: oi.LastModified,
		},
		IsLatest:       oi.IsLatest,
		IsDeleteMarker: oi.IsDeleteMarker,
		Version:        oi.VersionID,
	}
}
