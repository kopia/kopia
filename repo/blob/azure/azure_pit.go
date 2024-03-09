package azure

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobmodels "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/format"
)

type azPointInTimeStorage struct {
	azStorage

	pointInTime time.Time
}

func (az *azPointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	var (
		previousID blob.ID
		vs         []versionMetadata
	)

	err := az.listBlobVersions(ctx, blobIDPrefix, func(vm versionMetadata) error {
		if vm.BlobID != previousID {
			// different blob, process previous one
			if v, found := newestAtUnlessDeleted(vs, az.pointInTime); found {
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
		return errors.Wrapf(err, "could not list blob versions at time %s", az.pointInTime)
	}

	// process last blob
	if v, found := newestAtUnlessDeleted(vs, az.pointInTime); found {
		if err := cb(v.Metadata); err != nil {
			return err
		}
	}

	return nil
}

func (az *azPointInTimeStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// getMetadata returns the specific blob version at time t
	m, err := az.getVersionedMetadata(ctx, blobID)
	if err != nil {
		return errors.Wrap(err, "getting metadata")
	}

	return az.getBlobWithVersion(ctx, blobID, m.Version, offset, length, output)
}

// newestAtUnlessDeleted returns the last version in the list older than the PIT.
// Azure sorts in ascending order so return the last element in the list.
func newestAtUnlessDeleted(vs []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	vs = getOlderThan(vs, t)

	if len(vs) == 0 {
		return versionMetadata{}, false
	}

	v = vs[len(vs)-1]

	return v, !v.IsDeleteMarker
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in ascending
// timestamp order (and version order), unlike S3 which assumes descending.
// Versions in Azure follow the time.RFC3339Nano syntax.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	for i := range vs {
		if vs[i].Timestamp.After(t) {
			return vs[:i]
		}

		// The DeleteMarker blob takes the Timestamp of the previous version but has its own Version.
		// If there was a Kopia Delete Marker (the blob was protected) it will be caught above but if
		// the container has versioning enabled but no blob retention protection (or the blob was deleted outside
		// of the protection window) then we need to check the time of the VersionID because there could be a situation
		// where Azure's DeleteMarker version has Timestamp 2023-10-20 but Version 2023-10-27...then if PIT was 2023-10-22 the DeleteMarker
		// would be returned without this extra test
		if vs[i].IsDeleteMarker {
			versionTime, err := time.Parse(time.RFC3339Nano, vs[i].Version)
			if err != nil {
				return nil
			}

			if versionTime.After(t) {
				return vs[:i]
			}
		}
	}

	return vs
}

// listBlobVersions returns a list of blob versions but the blob is deleted, it returns Azure's delete marker version but excludes
// the Kopia delete marker version that is used to get around immutability protections.
func (az *azPointInTimeStorage) listBlobVersions(ctx context.Context, prefix blob.ID, callback func(vm versionMetadata) error) error {
	prefixStr := az.getObjectNameString(prefix)

	pager := az.service.NewListBlobsFlatPager(az.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefixStr,
		Include: azblob.ListBlobsInclude{
			Metadata:            true,
			DeletedWithVersions: true, // this shows DeleteMarkers aka blobs with HasVersionsOnly set to true
			Versions:            true,
		},
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return translateError(err)
		}

		for _, it := range page.Segment.BlobItems {
			vm, err := az.getVersionedBlobMeta(it)
			if err != nil {
				return translateError(err)
			}

			if err := callback(*vm); err != nil {
				return translateError(err)
			}
		}
	}

	return nil
}

func (az *azPointInTimeStorage) getVersionedMetadata(ctx context.Context, blobID blob.ID) (versionMetadata, error) {
	var vml []versionMetadata

	if err := az.getBlobVersions(ctx, blobID, func(vm versionMetadata) error {
		if !vm.Timestamp.After(az.pointInTime) {
			vml = append(vml, vm)
		}

		return nil
	}); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", blobID)
	}

	if v, found := newestAtUnlessDeleted(vml, az.pointInTime); found {
		return v, nil
	}

	return versionMetadata{}, blob.ErrBlobNotFound
}

// isAzureDeleteMarker checks for Azure created delete markers.
func (az *azPointInTimeStorage) isAzureDeleteMarker(it *azblobmodels.BlobItem) bool {
	var isDeleteMarker bool
	// HasVersionsOnly - Indicates that this root blob has been deleted
	if it.HasVersionsOnly != nil {
		isDeleteMarker = *it.HasVersionsOnly
	}

	return isDeleteMarker
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise, s is returned.
func maybePointInTimeStore(ctx context.Context, s *azStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
		return s, nil
	}

	pit := &azPointInTimeStorage{
		azStorage:   *s,
		pointInTime: *pointInTime, // not used for the check
	}

	err := pit.getBlobVersions(ctx, format.KopiaRepositoryBlobID, func(_ versionMetadata) error {
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "versioning must be enabled and a Kopia repository must exist")
	}

	return readonly.NewWrapper(pit), nil
}
