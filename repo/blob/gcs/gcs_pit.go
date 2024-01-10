package gcs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/pkg/errors"
)

type gcsPointInTimeStorage struct {
	gcsStorage

	pointInTime time.Time
}

func (gcs *gcsPointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	fmt.Printf("ListBlobs del pit: %s\n", blobIDPrefix)
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
			vs = vs[:0] // reset for next blob
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
	fmt.Printf("GetBlob del pit: %s\n", b)
	// getMetadata returns the specific blob version at time t
	m, err := gcs.getMetadata(ctx, b)
	if err != nil {
		return errors.Wrap(err, "getting metadata")
	}

	fmt.Printf("GetBlob del pit. faccio getBlobWithVersion di %s versione %s\n", b, m.Version)
	return gcs.getBlobWithVersion(ctx, b, m.Version, offset, length, output)
}

func (gcs *gcsPointInTimeStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	fmt.Printf("GetMetadata del pit %s\n", b)
	bm, err := gcs.getMetadata(ctx, b)

	return bm.Metadata, err
}

func (gcs *gcsPointInTimeStorage) getMetadata(ctx context.Context, b blob.ID) (versionMetadata, error) {
	fmt.Printf("getMetadata del pit %s\n", b)
	var vml []versionMetadata

	if err := gcs.getBlobVersions(ctx, b, func(m versionMetadata) error {
		fmt.Printf("  trovata versione %s del %s. deleted=%t\n", m.Version, m.Timestamp, m.IsDeleteMarker)
		// only include versions older than s.pointInTime
		if !m.Timestamp.After(gcs.pointInTime) {
			fmt.Printf("      Skippo versione %s del %s. Troppo nuova.\n", m.Version, m.Timestamp)
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
func newestAtUnlessDeleted(vs []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	fmt.Printf("newestAtUnlessDeleted del pit %s\n", t)

	for _, xxx := range vs {
		fmt.Printf("newestAtUnlessDeleted: found version %s del %s. deleted=%t\n", xxx.Version, xxx.Timestamp, xxx.IsDeleteMarker)
	}

	/*
		// Sort
		sort.Slice(vs, func(i, j int) bool {
			return vs[i].Timestamp.After(vs[j].Timestamp)
		})

		for _, xxx := range vs {
			fmt.Printf("POST: Versione %s del %s.\n", xxx.Version, xxx.Timestamp)
		}
	*/

	vs = getOlderThan(vs, t)

	if len(vs) == 0 {
		return versionMetadata{}, false
	}

	// v = vs[0] // versione s3
	v = vs[len(vs)-1] // versione azure

	fmt.Printf("newestAtUnlessDeleted del pit %s deleted=%t\n", v.Timestamp, v.IsDeleteMarker)

	//return v, !v.IsDeleteMarker
	return v, true
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in descending
// timestamp order.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	fmt.Printf("getOlderThan del pit %s \n", t)

	for i := range vs {
		if !vs[i].Timestamp.After(t) {
			fmt.Printf("  Skippo versione %s del %s. Troppo nuova.\n", vs[i].Version, vs[i].Timestamp)
			return vs[i:]
		}
	}

	return []versionMetadata{}
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, gcs *gcsStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := gcs.Options.PointInTime; pit == nil || pit.IsZero() {
		return gcs, nil
	}

	// Does the bucket supports versioning?
	attrs, err := gcs.bucket.Attrs(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if bucket '%s' supports versioning", gcs.BucketName)
	}

	if !attrs.VersioningEnabled {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", gcs.BucketName)
	}

	fmt.Printf("Versioning attivo su %s, instanzio il wrapper\n", gcs.BucketName)

	return readonly.NewWrapper(&gcsPointInTimeStorage{
		gcsStorage:  *gcs,
		pointInTime: *pointInTime,
	}), nil
}
