package s3

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type s3PointInTimeStorage struct {
	s3Storage

	pointInTime time.Time
}

func (s *s3PointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	var (
		previousID blob.ID
		vs         []versionMetadata
	)

	err := s.listBlobVersions(ctx, blobIDPrefix, func(vm versionMetadata) error {
		if vm.BlobID != previousID {
			// different blob, process previous one
			if v, found := newestAtUnlessDeleted(vs, s.pointInTime); found {
				if err := cb(v.Metadata); err != nil {
					return err
				}
			}

			previousID = vm.BlobID
			vs = vs[:0] // reset for next blob
		}

		vs = append(vs, vm)

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "could not list blob versions at time %s", s.pointInTime)
	}

	// process last blob
	if v, found := newestAtUnlessDeleted(vs, s.pointInTime); found {
		if err := cb(v.Metadata); err != nil {
			return err
		}
	}

	return nil
}

func (s *s3PointInTimeStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// getMetadata returns the specific blob version at time t
	m, err := s.getMetadata(ctx, blobID)
	if err != nil {
		return err
	}

	return s.getBlobWithVersion(ctx, blobID, m.Version, offset, length, output)
}

func (s *s3PointInTimeStorage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	m, err := s.getMetadata(ctx, blobID)

	return m.Metadata, err
}

func (s *s3PointInTimeStorage) getMetadata(ctx context.Context, blobID blob.ID) (versionMetadata, error) {
	var vml []versionMetadata

	if err := s.getBlobVersions(ctx, blobID, func(m versionMetadata) error {
		// only include versions older than s.pointInTime
		if !m.Timestamp.After(s.pointInTime) {
			vml = append(vml, m)
		}

		return nil
	}); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", blobID)
	}

	if v, found := newestAtUnlessDeleted(vml, s.pointInTime); found {
		return v, nil
	}

	return versionMetadata{}, blob.ErrBlobNotFound
}

func newestAtUnlessDeleted(vs []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	vs = getOlderThan(vs, t)

	if len(vs) == 0 {
		return versionMetadata{}, false
	}

	v = vs[0]

	return v, !v.IsDeleteMarker
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in descending
// timestamp order.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	for i := range vs {
		if !vs[i].Timestamp.After(t) {
			return vs[i:]
		}
	}

	return []versionMetadata{}
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, s *s3Storage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
		return s, nil
	}

	// Does the bucket supports versioning?
	vi, err := s.cli.GetBucketVersioning(ctx, s.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if bucket '%s' supports versioning", s.BucketName)
	}

	if !vi.Enabled() {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", s.BucketName)
	}

	return readonly.NewWrapper(&s3PointInTimeStorage{
		s3Storage:   *s,
		pointInTime: *pointInTime,
	}), nil
}
