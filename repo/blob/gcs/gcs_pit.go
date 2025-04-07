package gcs

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type gcsPointInTimeStorage struct {
	gcsStorage

	pointInTime time.Time
}

func (gcs *gcsPointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	var (
		previousID blob.ID
		vs         []versionMetadata
	)

	err := gcs.listBlobVersions(ctx, blobIDPrefix, func(vm versionMetadata) error {
		if vm.BlobID != previousID {
			// different blob, process previous one
			if v, found := newestAtUnlessDeleted(vs, gcs.pointInTime); found {
				if err := cb(v.Metadata); err != nil {
					return err
				}
			}

			previousID = vm.BlobID
			vs = vs[:0] // reset for next blob to reuse the slice storage whenever possible and avoid unnecessary allocations.
		}

		vs = append(vs, vm)

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "could not list blob versions at time %s", gcs.pointInTime)
	}

	// process last blob
	if v, found := newestAtUnlessDeleted(vs, gcs.pointInTime); found {
		if err := cb(v.Metadata); err != nil {
			return err
		}
	}

	return nil
}

func (gcs *gcsPointInTimeStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// getVersionedMetadata returns the specific blob version at time t
	m, err := gcs.getVersionedMetadata(ctx, b)
	if err != nil {
		return errors.Wrap(err, "getting metadata")
	}

	return gcs.getBlobWithVersion(ctx, b, m.Version, offset, length, output)
}

func (gcs *gcsPointInTimeStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bm, err := gcs.getVersionedMetadata(ctx, b)

	return bm.Metadata, err
}

func (gcs *gcsPointInTimeStorage) getVersionedMetadata(ctx context.Context, b blob.ID) (versionMetadata, error) {
	var vml []versionMetadata

	if err := gcs.getBlobVersions(ctx, b, func(m versionMetadata) error {
		// only include versions older than s.pointInTime
		if !m.Timestamp.After(gcs.pointInTime) {
			vml = append(vml, m)
		}

		return nil
	}); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", b)
	}

	if v, found := newestAtUnlessDeleted(vml, gcs.pointInTime); found {
		return v, nil
	}

	return versionMetadata{}, blob.ErrBlobNotFound
}

// newestAtUnlessDeleted returns the last version in the list older than the PIT.
// Google sorts in ascending order so return the last element in the list.
func newestAtUnlessDeleted(vx []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	vs := getOlderThan(vx, t)

	if len(vs) == 0 {
		return versionMetadata{}, false
	}

	v = vs[len(vs)-1]

	return v, !v.IsDeleteMarker
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in ascending
// timestamp order like Azure and unlike S3, which assumes descending.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	for i := range vs {
		if vs[i].Timestamp.After(t) {
			return vs[:i]
		}
	}

	return vs
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, gcs *gcsStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := gcs.Options.PointInTime; pit == nil || pit.IsZero() {
		return gcs, nil
	}

	attrs, err := gcs.bucket.Attrs(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if bucket '%s' supports versioning", gcs.BucketName)
	}

	if !attrs.VersioningEnabled {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", gcs.BucketName)
	}

	return readonly.NewWrapper(&gcsPointInTimeStorage{
		gcsStorage:  *gcs,
		pointInTime: *pointInTime,
	}), nil
}
