// Package pit provides a Point-in-time view of a versioned blob store
package pit

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type pitStorage struct {
	blob.Storage

	vs          VersionedStorage
	pointInTime time.Time
}

func (s pitStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	var (
		previousID blob.ID
		vs         []VersionMetadata
	)

	err := s.vs.ListBlobVersions(ctx, blobIDPrefix, func(vm VersionMetadata) error {
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

func (s pitStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64) ([]byte, error) {
	// getMetadata returns the specific blob version at time t
	m, err := s.getMetadata(ctx, blobID)
	if err != nil {
		return nil, err
	}

	b, err := s.vs.GetBlobWithVersion(ctx, blobID, m.Version, offset, length)

	return b, errors.Wrapf(err, "could not get blob: %s, version: %s", blobID, m.Version)
}

func (s pitStorage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	m, err := s.getMetadata(ctx, blobID)

	return m.Metadata, err
}

func (s pitStorage) getMetadata(ctx context.Context, blobID blob.ID) (VersionMetadata, error) {
	var vml []VersionMetadata

	if err := s.vs.GetBlobVersions(ctx, blobID, func(m VersionMetadata) error {
		// only include versions older than s.pointInTime
		if !m.Timestamp.After(s.pointInTime) {
			vml = append(vml, m)
		}

		return nil
	}); err != nil {
		return VersionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", blobID)
	}

	if v, found := newestAtUnlessDeleted(vml, s.pointInTime); found {
		return v, nil
	}

	return VersionMetadata{}, blob.ErrBlobNotFound
}

func newestAtUnlessDeleted(vs []VersionMetadata, t time.Time) (v VersionMetadata, found bool) {
	vs = getOlderThan(vs, t)

	if wasBlobDeleted(vs) {
		return VersionMetadata{}, false
	}

	return vs[len(vs)-1], true
}

// Removes versions that are newer than t. The filtering is done in place and
// and uses the same slice storage as vs. Versions in vs are in descending
// timestamp order.
func getOlderThan(vs []VersionMetadata, t time.Time) []VersionMetadata {
	for i := range vs {
		if !vs[i].Timestamp.After(t) {
			return vs[i:]
		}
	}

	return nil
}

// A blob is considered deleted if either there is no version for it, that is,
// vs is empty; or there is at least one deletion marker in vs, even if it is
// not the most recent version.
func wasBlobDeleted(vs []VersionMetadata) bool {
	if len(vs) == 0 {
		return true
	}

	for _, v := range vs {
		if v.IsDeleteMarker {
			return true
		}
	}

	return false
}

// NewWrapper wraps s with a PiT store when s is versioned and pit is non-zero.
// Otherwise it returns s unmodified.
func NewWrapper(ctx context.Context, s blob.Storage, pointInTime *time.Time) (blob.Storage, error) {
	if pointInTime == nil || pointInTime.IsZero() {
		return s, nil
	}

	// Check if the bucket supports versioning when a point in time is specified
	if v, ok := s.(VersionedStorage); ok {
		switch versioned, err := v.IsVersioned(ctx); {
		case err != nil:
			return nil, errors.Wrap(err, "Coud not determine if the bucket is versioned")
		case !versioned:
			return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket store")
		}

		return readonly.NewWrapper(&pitStorage{
			Storage:     s,
			pointInTime: *pointInTime,
			vs:          v,
		}), nil
	}

	return s, nil
}
