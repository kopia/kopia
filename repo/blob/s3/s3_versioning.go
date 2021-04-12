package s3

import (
	"context"

	"github.com/pkg/errors"
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
