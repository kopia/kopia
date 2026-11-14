package azure

import (
	"testing"
	"time"

	azblobmodels "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/require"
)

func testVersion(ts time.Time, version string) versionMetadata {
	return versionMetadata{
		BlobID:    "blob-id",
		Length:    10,
		Timestamp: ts,
		Version:   version,
	}
}

func testDeleteMarker(ts time.Time, version string) versionMetadata {
	vm := testVersion(ts, version)
	vm.Length = 0
	vm.IsDeleteMarker = true

	return vm
}

func TestNewestAtUnlessDeleted_DeleteMarkerWithoutVersion(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 10, 13, 14, 0, 4, 0, time.UTC)
	deleted := created.Add(2 * time.Minute)

	// a delete marker created through the temp blob path takes the time of the temp blob, which is after the blob.
	vs := func() []versionMetadata {
		return []versionMetadata{
			testVersion(created, "v1"),
			testDeleteMarker(deleted, ""),
		}
	}

	_, found := newestAtUnlessDeleted(vs(), created.Add(-time.Second))
	require.False(t, found, "before the blob was written")

	got, found := newestAtUnlessDeleted(vs(), created)
	require.True(t, found)
	require.Equal(t, "v1", got.Version)

	got, found = newestAtUnlessDeleted(vs(), deleted.Add(-time.Second))
	require.True(t, found, "between the write and the delete")
	require.Equal(t, "v1", got.Version)

	_, found = newestAtUnlessDeleted(vs(), deleted)
	require.False(t, found, "at the delete")

	_, found = newestAtUnlessDeleted(vs(), deleted.Add(time.Hour))
	require.False(t, found, "after the delete")
}

func TestNewestAtUnlessDeleted_DeleteMarkerSharingTimestampWithVersion(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 10, 13, 14, 0, 4, 0, time.UTC)

	// a direct delete leaves the marker with the blob's own timestamp, so the delete time is unknown and the blob is not treated as deleted.
	inOrder := []versionMetadata{
		testVersion(ts, "v1"),
		testDeleteMarker(ts, ""),
	}

	got, found := newestAtUnlessDeleted(inOrder, ts.Add(time.Hour))
	require.True(t, found)
	require.Equal(t, "v1", got.Version)

	markerFirst := []versionMetadata{
		testDeleteMarker(ts, ""),
		testVersion(ts, "v1"),
	}

	got, found = newestAtUnlessDeleted(markerFirst, ts.Add(time.Hour))
	require.True(t, found, "marker listed before the version")
	require.Equal(t, "v1", got.Version)
}

func TestGetOlderThan_DeleteMarkerWithUnparsableVersionKeepsEarlierVersions(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 10, 13, 14, 0, 4, 0, time.UTC)

	vs := []versionMetadata{
		testVersion(ts, "v1"),
		testDeleteMarker(ts.Add(time.Minute), "not-a-time"),
	}

	got := getOlderThan(vs, ts.Add(time.Hour))
	require.Len(t, got, 2)
}

func TestGetVersionedBlobMeta(t *testing.T) {
	t.Parallel()

	az := &azPointInTimeStorage{}
	lastModified := time.Date(2026, 10, 13, 14, 2, 5, 0, time.UTC)
	name := "blob-id"
	length := int64(0)
	yes := true
	version := "2026-10-13T14:00:04.9124967Z"

	item := func(versionID *string, hasVersionsOnly *bool) *azblobmodels.BlobItem {
		return &azblobmodels.BlobItem{
			Name:            &name,
			VersionID:       versionID,
			HasVersionsOnly: hasVersionsOnly,
			Properties:      &azblobmodels.BlobProperties{ContentLength: &length, LastModified: &lastModified},
		}
	}

	vm, err := az.getVersionedBlobMeta(item(nil, &yes))
	require.NoError(t, err, "delete marker without VersionID")
	require.True(t, vm.IsDeleteMarker)
	require.Empty(t, vm.Version)
	require.Equal(t, lastModified, vm.Timestamp)

	vm, err = az.getVersionedBlobMeta(item(&version, nil))
	require.NoError(t, err)
	require.False(t, vm.IsDeleteMarker)
	require.Equal(t, version, vm.Version)

	_, err = az.getVersionedBlobMeta(item(nil, nil))
	require.Error(t, err, "versioning disabled: a blob without VersionID")
}
