package s3

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

var versionedProviders = []string{"S3-Versioned", "Wasabi-Versioned"}

func TestGetBlobVersions(t *testing.T) {
	const (
		batch1Count  = 3
		batch2Count  = 4
		batch3Count  = 5
		versionCount = batch1Count + batch2Count + batch3Count
	)

	t.Parallel()

	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			s := getVersionedTestStore(t, env)
			ctx := testlogging.Context(t)
			blobName := "b1-" + randBlobName()
			blobs := makeBlobVersions(t, blobName, versionCount)
			bm := putBlobs(ctx, t, s, blobs[:batch1Count])

			testGetBlobVersions(ctx, t, s, blobName, bm)

			bm2 := putBlobs(ctx, t, s, blobs[batch1Count:batch1Count+batch2Count])
			bm = append(bm, bm2...)

			testGetBlobVersions(ctx, t, s, blobName, bm)

			// delete a blob
			dm, err := deleteBlob(ctx, s, blobName)

			require.NoError(t, err)
			bm = append(bm, dm)

			var b gather.WriteBuffer

			// GetBlob should return not found but all the versions should be available
			err = s.GetBlob(ctx, blobName, 0, -1, &b)

			require.ErrorIs(t, err, blob.ErrBlobNotFound)
			testGetBlobVersions(ctx, t, s, blobName, bm)

			bm3 := putBlobs(ctx, t, s, blobs[batch1Count+batch2Count:])
			bm = append(bm, bm3...)

			testGetBlobVersions(ctx, t, s, blobName, bm)

			// list with a prefix of the blob or a random suffix should return no blob versions
			got, err := getBlobVersions(ctx, s, blobName+"foo")

			require.ErrorIs(t, err, blob.ErrBlobNotFound)
			require.Empty(t, got)

			// list with a prefix of the blob or a random suffix should return no blob versions
			got, err = getBlobVersions(ctx, s, "non-existing-foo-prefix")

			require.ErrorIs(t, err, blob.ErrBlobNotFound)
			require.Empty(t, got)

			// list with a prefix of the blob or a random suffix should return no blob versions
			got, err = getBlobVersions(ctx, s, "non-existing-foo-prefix"+blobName)

			require.ErrorIs(t, err, blob.ErrBlobNotFound)
			require.Empty(t, got)

			// list all versions should get the ones created so far
			testListAllVersions(ctx, t, s, "", bm)
		})
	}
}

// TestGetDifferentBlobVersions verifies the behavior of getting versions from
// different blobs.
func TestGetDifferentBlobVersions(t *testing.T) {
	t.Parallel()

	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			ctx := testlogging.Context(t)
			s := getVersionedTestStore(t, env)

			// Setup: blobs with different names, multiple versions each
			blobs := makeBlobsWithVersions(t, "", []int{5, 8, 3})

			var (
				allMetas  []versionMetadata
				blobMetas [][]versionMetadata
			)

			for _, b := range blobs {
				bm := putBlobs(ctx, t, s, b)
				blobMetas = append(blobMetas, bm)
				allMetas = append(allMetas, bm...)
			}

			// GetBlobVersions should return only the ones for each blob
			for _, bm := range blobMetas {
				testGetBlobVersions(ctx, t, s, bm[0].BlobID, bm)
			}

			// List with no prefix should get all of them
			testListAllVersions(ctx, t, s, "", allMetas)
		})
	}
}

// Test listing blob versions with the given blob name prefix.
func TestSingleBlobVersionsListPrefixes(t *testing.T) {
	const versionCount = 5

	t.Parallel()

	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			ctx := testlogging.Context(t)
			s := getVersionedTestStore(t, env)
			blobName := randBlobName()
			blobs := makeBlobVersions(t, blobName, versionCount)
			bm := putBlobs(ctx, t, s, blobs)

			testGetBlobVersions(ctx, t, s, blobName, bm)

			// check get blob versions with different blob names and prefixes
			blobPrefixes := allPrefixes(blobName)

			// list all versions should succeed for all valid prefixes
			for _, n := range blobPrefixes {
				testListAllVersions(ctx, t, s, n, bm)
			}

			// list all versions should return no versions for non-prefixes
			nonPrefixes := []blob.ID{
				"_non-existent", // guaranteed to not exist since generated blob ids are hex digits
				blob.ID(randHex(t, 3)) + blobName,
				blobName + blob.ID(randHex(t, 5)),
			}

			for _, n := range nonPrefixes {
				versions, err := listBlobVersions(ctx, s, n)
				require.NoError(t, err)
				require.Empty(t, versions, "expected empty versions")
			}

			nonExistentBlobIDs := append([]blob.ID(nil), blobPrefixes[:len(blobPrefixes)-1]...)
			nonExistentBlobIDs = append(nonExistentBlobIDs, nonPrefixes...)

			// getting versions for these IDs should err with ErrBlobNotFound
			for _, n := range nonExistentBlobIDs {
				versions, err := getBlobVersions(ctx, s, n)

				require.ErrorIs(t, err, blob.ErrBlobNotFound)
				require.Empty(t, versions, "expected empty versions")
			}
		})
	}
}

// Tests blob listing by prefixes for blobs with different prefixes.
func TestListMultipleBlobPrefixes(t *testing.T) {
	t.Parallel()

	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			ctx := testlogging.Context(t)
			s := getVersionedTestStore(t, env)

			// Setup: blobs with different names 2 of them with a common
			// prefix, multiple versions each
			blobsx := makeBlobsWithVersions(t, "x-", []int{5, 3})
			blobsy := makeBlobsWithVersions(t, "y-", []int{2})

			var bmx, bmy, allMetas []versionMetadata

			for _, b := range blobsx {
				bm := putBlobs(ctx, t, s, b)
				bmx = append(bmx, bm...)
			}

			for _, b := range blobsy {
				bm := putBlobs(ctx, t, s, b)
				bmy = append(bmy, bm...)
			}

			allMetas = append(allMetas, bmx...)
			allMetas = append(allMetas, bmy...)

			// List with no prefix should get all of them
			testListAllVersions(ctx, t, s, "", allMetas)

			// List with common prefix should return only include the corresponding blobs
			testListAllVersions(ctx, t, s, "x-", bmx)
			testListAllVersions(ctx, t, s, "y-", bmy)
		})
	}
}

func TestGetBlobWithVersion(t *testing.T) {
	t.Parallel()

	// set up the state 3 blob names with a bunch of versions each,
	// then retrieve each blob version and verify its contents
	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			ctx := testlogging.Context(t)
			s := getVersionedTestStore(t, env)
			blobs := makeBlobsWithVersions(t, "", []int{6, 5, 3})

			var metas []versionMetadata

			for _, b := range blobs {
				bm := putBlobs(ctx, t, s, b)
				metas = append(metas, bm...)
			}

			var i int
			for _, b := range blobs {
				for _, bv := range b {
					m := metas[i]
					require.Equal(t, bv.id, m.BlobID) // test safeguard

					var b gather.WriteBuffer

					err := s.getBlobWithVersion(ctx, m.BlobID, m.Version, 0, -1, &b)

					require.NoError(t, err)

					c, err := io.ReadAll(bv.contents(t).Reader())

					require.NoError(t, err)
					require.Equal(t, c, b.ToByteSlice())
					i++
				}
			}
		})
	}
}

func TestGetVersionMetadata(t *testing.T) {
	t.Parallel()

	const versionCount = 7

	for _, provider := range versionedProviders {
		env := providerCreds[provider]

		t.Run(provider, func(t *testing.T) {
			ctx := testlogging.Context(t)
			s := getVersionedTestStore(t, env)
			blobName := "b1-" + randBlobName()
			blobs := makeBlobVersions(t, blobName, versionCount)

			bm := putBlobs(ctx, t, s, blobs)

			// check current version metadata matches last added blob version
			m, err := s.getVersionMetadata(ctx, blobName, "")

			require.NoError(t, err)
			compareMetadata(t, bm[len(bm)-1], m)

			for _, b := range bm {
				// check version metadata matches for each blob version
				m, err = s.getVersionMetadata(ctx, blobName, b.Version)

				require.NoError(t, err)
				require.Equal(t, blobName, m.BlobID, "blob names must match")
				compareMetadata(t, b, m)
			}
		})
	}
}

func TestInfoToVersionMetadata(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2021, time.February, 27, 10, 38, 5, 0, time.Local)

	cases := []struct {
		prefix     string
		objectInfo minio.ObjectInfo
		expected   versionMetadata
	}{
		{},
		{
			"some/prefix/x/",
			minio.ObjectInfo{
				ETag:           "",
				Key:            "some/prefix/x/blob-id",
				LastModified:   timestamp,
				Size:           78901,
				IsLatest:       true,
				IsDeleteMarker: true,
				VersionID:      "version-identifier",
			},
			versionMetadata{
				Metadata: blob.Metadata{
					BlobID:    "blob-id",
					Length:    78901,
					Timestamp: timestamp,
				},
				IsLatest:       true,
				IsDeleteMarker: true,
				Version:        "version-identifier",
			},
		},
		{
			"some/prefix/x",
			minio.ObjectInfo{
				ETag:           "",
				Key:            "some/prefix/xblob-2",
				LastModified:   timestamp,
				Size:           78901,
				IsLatest:       false,
				IsDeleteMarker: false,
				VersionID:      "",
			},
			versionMetadata{
				Metadata: blob.Metadata{
					BlobID:    "blob-2",
					Length:    78901,
					Timestamp: timestamp,
				},
				IsLatest:       false,
				IsDeleteMarker: false,
				Version:        "",
			},
		},
	}

	for _, tc := range cases {
		vm := infoToVersionMetadata(tc.prefix, &tc.objectInfo)
		require.Equal(t, tc.expected, vm)
	}
}

func TestGetOlderThan(t *testing.T) {
	t.Parallel()

	base := clock.Now().UTC().Truncate(time.Second)
	vs := makeVersionsMetadata(t, blob.ID("blobfux"), 11, base)
	want := append([]versionMetadata(nil), vs...)

	// entries are at least 2 seconds apart
	for i, v := range vs {
		got := getOlderThan(vs, v.Timestamp.Add(time.Second))
		compareVersions(t, got, want[i:])

		got = getOlderThan(vs, v.Timestamp)
		compareVersions(t, got, want[i:])

		got = getOlderThan(vs, v.Timestamp.Add(-time.Second))
		compareVersions(t, got, want[i+1:])
	}
}

func TestGetOlderThanSameTime(t *testing.T) {
	t.Parallel()

	base := clock.Now().UTC().Truncate(time.Second)
	vs := makeVersionsMetadata(t, blob.ID("foobarx"), 4, base)

	// two consecutive versions with the same timestamp
	vt := vs[2].Timestamp
	vs[1].Timestamp = vt

	// both should be included for t >= vs[1]
	want := append([]versionMetadata(nil), vs[1:]...)
	got := getOlderThan(vs, vt.Add(time.Second))

	compareVersions(t, got, want)

	got = getOlderThan(vs, vt)

	compareVersions(t, got, want)

	got = getOlderThan(vs, vt.Add(-time.Second))

	// both blob versions (vs[1] and vs[2]) should be excluded for t < vt
	compareVersions(t, got, want[2:])
}

func TestNewestAtUnlessDeleted_NonDeleted(t *testing.T) {
	t.Parallel()

	base := clock.Now().UTC().Truncate(time.Second)
	vs := makeVersionsMetadata(t, blob.ID("blobfux"), 11, base)

	for i, v := range vs {
		got, found := newestAtUnlessDeleted(vs, v.Timestamp.Add(time.Second))
		require.True(t, found)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp)
		require.True(t, found)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp.Add(-time.Second))

		if i == len(vs)-1 {
			require.False(t, found)
			require.Zero(t, got)

			continue
		}

		require.True(t, found)
		require.Equal(t, vs[i+1], got)
	}
}

func TestNewestAtUnlessDeleted_Deleted(t *testing.T) {
	t.Parallel()

	base := clock.Now().UTC().Truncate(time.Second)
	vs := makeVersionsMetadata(t, blob.ID("blobfux"), 5, base)

	// insert a couple of deletion markers
	i := rand.Intn(len(vs))
	vs[i].IsDeleteMarker = true
	vs[i].Length = 0

	i = rand.Intn(len(vs))
	vs[i].IsDeleteMarker = true
	vs[i].Length = 0

	for i, v := range vs {
		got, found := newestAtUnlessDeleted(vs, v.Timestamp.Add(time.Second))
		require.Equal(t, !v.IsDeleteMarker, found)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp)
		require.Equal(t, !v.IsDeleteMarker, found)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp.Add(-time.Second))

		if i == len(vs)-1 {
			require.False(t, found)
			require.Zero(t, got)

			continue
		}

		require.Equal(t, !vs[i+1].IsDeleteMarker, found)
		require.Equal(t, vs[i+1], got)
	}
}

func TestNewestAtUnlessDeleted_AllDeleted(t *testing.T) {
	t.Parallel()

	base := clock.Now().UTC().Truncate(time.Second)
	vs := makeVersionsMetadata(t, blob.ID("blobfux"), 5, base)

	// make them all deletion markers
	for i := range vs {
		vs[i].IsDeleteMarker = true
		vs[i].Length = 0
	}

	for i, v := range vs {
		got, found := newestAtUnlessDeleted(vs, v.Timestamp.Add(time.Second))

		require.False(t, found, "i: %d", i)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp)

		require.False(t, found)
		require.Equal(t, v, got)

		got, found = newestAtUnlessDeleted(vs, v.Timestamp.Add(-time.Second))

		require.False(t, found)

		if i == len(vs)-1 {
			require.Zero(t, got)
			continue
		}

		require.Equal(t, vs[i+1], got)
	}
}

func testListAllVersions(ctx context.Context, tb testing.TB, s *s3Storage, prefix blob.ID, want []versionMetadata) {
	tb.Helper()

	got, err := listBlobVersions(ctx, s, prefix)

	require.NoError(tb, err)
	require.NotNil(tb, got)

	// pre-process 'want' before comparing versions
	compareVersionSlices(tb, creationToListingVersionsOrder(want), got)
}

func testGetBlobVersions(ctx context.Context, tb testing.TB, s *s3Storage, blobName blob.ID, want []versionMetadata) {
	tb.Helper()

	got, err := getBlobVersions(ctx, s, blobName)

	require.NoError(tb, err)
	require.NotNil(tb, got)
	compareVersionSlices(tb, want, reverseVersionSlice(got))
}

func compareVersions(t *testing.T, got, want []versionMetadata) {
	t.Helper()

	require.Equal(t, want, got, "version metadata differs")
}

// Generated versions are in creation time descending order and at least
// 2 seconds apart.
func makeVersionsMetadata(t *testing.T, blobID blob.ID, n int, base time.Time) []versionMetadata {
	t.Helper()

	if n == 0 {
		return nil
	}

	vs := make([]versionMetadata, n)
	ct := base

	zeroPadding := int(math.Log10(float64(n))) + 1

	for i := range vs {
		vs[i].BlobID = blobID
		vs[i].Version = fmt.Sprintf("%0*d-%02x", zeroPadding, n-i, rand.Intn(256))
		vs[i].Timestamp = ct
		vs[i].Length = int64(rand.Int31())

		ct = ct.Add(-time.Duration((2 + rand.Int63n(10)) * int64(time.Second)))
	}

	vs[0].IsLatest = true

	return vs
}

type blobContent struct {
	id       blob.ID
	contents func(testing.TB) blob.Bytes
}

func makeBlobsWithVersions(tb testing.TB, prefix blob.ID, versionCounts []int) [][]blobContent {
	tb.Helper()

	blobs := make([][]blobContent, len(versionCounts))

	for i, count := range versionCounts {
		blobs[i] = makeBlobVersions(tb, prefix+randBlobName(), count)
	}

	return blobs
}

func makeBlobVersions(tb testing.TB, name blob.ID, count int) []blobContent {
	tb.Helper()

	blobs := make([]blobContent, count)

	for i := range blobs {
		blobs[i] = makeBlobVersion(tb, name, i, randHex(tb, 2+rand.Intn(5)))
	}

	require.Len(tb, blobs, count, "unexpected number of blobs")
	require.NotNil(tb, blobs)

	return blobs
}

func makeBlobVersion(tb testing.TB, name blob.ID, seq int, additionalContent string) blobContent {
	tb.Helper()

	return blobContent{
		id: name,
		contents: func(tb testing.TB) blob.Bytes {
			tb.Helper()

			return genContentBytes(tb, name, seq, additionalContent)
		},
	}
}

func genContentBytes(tb testing.TB, name blob.ID, seq int, additionalContent string) blob.Bytes {
	tb.Helper()

	var b gather.WriteBuffer

	_, err := fmt.Fprintf(&b, "%v %v %v", seq, name, additionalContent)
	require.NoError(tb, err)

	return b.Bytes()
}

func randBlobName() blob.ID {
	return blob.ID(fmt.Sprintf("%0x", rand.Uint64()))
}

func randHex(tb testing.TB, length int) string {
	tb.Helper()

	if length <= 16 {
		return fmt.Sprintf("%0x", rand.Uint64())[0:length]
	}

	return randLongHex(tb, length)
}

// Protects the initialization of a random number generator for byte sequences
// that are larger than 16 characters (8 bytes)
// Notice that this is not a crypto RNG.
var (
	rMu sync.Mutex
	// +checklocks:rMu
	r = rand.New(rand.NewSource(clock.Now().UnixNano()))
)

func randLongHex(tb testing.TB, length int) string {
	tb.Helper()

	byteLength := (length + 1) / 2
	b := make([]byte, byteLength)

	rMu.Lock()
	n, err := r.Read(b)
	rMu.Unlock()

	require.NoError(tb, err)
	require.Equal(tb, byteLength, n, "unexpected number of bytes while reading RNG")

	return hex.EncodeToString(b)[:length]
}

func mapBlobIDToVersions(vs []versionMetadata) map[string][]versionMetadata {
	m := make(map[string][]versionMetadata)

	for _, v := range vs {
		l := m[string(v.BlobID)]
		m[string(v.BlobID)] = append(l, v)
	}

	return m
}

// list blob versions returns blobs sorted by name/id and then by descending
// creation order (newest first, oldest last).
// The version metadata needs to be sorted before comparing versions.
// The assumption is that the versions in vs are in the creation order for
// versions for the same blob.
func creationToListingVersionsOrder(vs []versionMetadata) []versionMetadata {
	m := mapBlobIDToVersions(vs)

	ids := make([]string, 0, len(m))

	for k := range m {
		ids = append(ids, k)
	}

	// sorting by id, desc(creation time) is NOT what's desired because multiple
	// versions for the same blob may have the same creation time. Instead, the
	// versions for the same blobs must be simply reversed from the order they
	// are in 'want', which is expected to be the creation order for the blobs
	sort.Strings(ids)

	listOrder := make([]versionMetadata, 0, len(vs))

	for _, id := range ids {
		listOrder = append(listOrder, reverseVersionSlice(m[id])...)
	}

	return listOrder
}

func putBlobs(ctx context.Context, tb testing.TB, s *s3Storage, blobs []blobContent) []versionMetadata {
	tb.Helper()

	vm := make([]versionMetadata, len(blobs))

	for i, b := range blobs {
		m, err := s.putBlobVersion(ctx, b.id, b.contents(tb), blob.PutOptions{})
		if err != nil {
			tb.Fatalf("can't put blob: %v", err)
			continue
		}

		vm[i] = m
	}

	return vm
}

// only available for tests.
func (s *s3Storage) putBlobVersion(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) (versionMetadata, error) {
	return retry.WithExponentialBackoff(ctx, "putBlobVersion("+string(id)+")", func() (versionMetadata, error) {
		return s.putBlob(ctx, id, data, opts)
	}, isRetriable)
}

func compareMetadata(tb testing.TB, a, b versionMetadata) {
	tb.Helper()

	// Not comparing timestamps because that is not returned during put blob,
	// at least not by AWS S3 or the MinIO SDK.
	// Not comparing IsLatest because that changes over time.
	require.Equalf(tb, a.BlobID, b.BlobID, "blob ids do not match a:%v b:%v", a, b)
	require.Equalf(tb, a.Length, b.Length, "blob lengths do not match a:%v b:%v", a, b)
	require.Equalf(tb, a.IsDeleteMarker, b.IsDeleteMarker, "blob delete marker do not match a:%v b:%v", a, b)

	// compare versions only for non-deletion markers because the
	// deletion-marker metadata is not returned by the delete blob operation,
	// and can only be retrieved later by listing versions.
	if !a.IsDeleteMarker {
		require.Equalf(tb, a.Version, b.Version, "blob versions do not match a:%v b:%v", a, b)
	}
}

func compareVersionSlices(tb testing.TB, a, b []versionMetadata) {
	tb.Helper()

	l := len(a)

	if len(b) < l {
		l = len(b)
	}

	for i := range a[:l] {
		compareMetadata(tb, a[i], b[i])
	}

	require.Equal(tb, len(a), len(b), "the number of the blob versions to compare does not match", a, b)
}

func reverseVersionSlice(m []versionMetadata) []versionMetadata {
	r := make([]versionMetadata, len(m))

	for i, v := range m {
		r[len(m)-1-i] = v
	}

	return r
}

func allPrefixes(n blob.ID) []blob.ID {
	p := make([]blob.ID, len(n)+1)

	for i := range n {
		p[i] = n[:i]
	}

	p[len(n)] = n

	return p
}

// getBlobVersions returns version metadata for a blob.
func getBlobVersions(ctx context.Context, s *s3Storage, b blob.ID) ([]versionMetadata, error) {
	var vml []versionMetadata

	if err := s.getBlobVersions(ctx, b, func(m versionMetadata) error {
		vml = append(vml, m)

		return nil
	}); err != nil {
		return nil, errors.Wrapf(err, "could not get version metadata for blobs with id %s", b)
	}

	return vml, nil
}

func listBlobVersions(ctx context.Context, s *s3Storage, prefix blob.ID) ([]versionMetadata, error) {
	var vml []versionMetadata

	if err := s.listBlobVersions(ctx, prefix, func(m versionMetadata) error {
		vml = append(vml, m)

		return nil
	}); err != nil {
		return nil, errors.Wrapf(err, "could not get version metadata for blobs with prefix %s", prefix)
	}

	return vml, nil
}

func deleteBlob(ctx context.Context, s blob.Storage, b blob.ID) (versionMetadata, error) {
	if err := s.DeleteBlob(ctx, b); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not delete blob %q", b)
	}

	// length is 0, timestamp and version are unknown
	return versionMetadata{
		Metadata:       blob.Metadata{BlobID: b},
		IsDeleteMarker: true,
	}, nil
}

func isRetriable(err error) bool {
	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		return false

	case errors.Is(err, blob.ErrInvalidRange):
		return false

	case errors.Is(err, blob.ErrSetTimeUnsupported):
		return false

	default:
		return true
	}
}

func getVersionedTestStore(tb testing.TB, envName string) *s3Storage {
	tb.Helper()

	ctx := testlogging.Context(tb)
	o := getProviderOptions(tb, envName)
	o.Prefix = path.Join(tb.Name(), uuid.NewString()) + "/"

	s, err := newStorage(ctx, o)
	require.NoError(tb, err, "error creating versioned store client")

	tb.Cleanup(func() {
		cleanupVersions(tb, s)
		blobtesting.CleanupOldData(ctx, tb, s, 0)
	})

	return s
}

func cleanupVersions(tb testing.TB, s *s3Storage) {
	tb.Helper()

	ctx := testlogging.Context(tb)
	ch := make(chan minio.ObjectInfo, 4)
	errChan := s.cli.RemoveObjects(ctx, s.BucketName, ch, minio.RemoveObjectsOptions{})

	err := s.listBlobVersions(ctx, "", func(m versionMetadata) error {
		ch <- minio.ObjectInfo{
			Key:       s.Prefix + string(m.BlobID),
			VersionID: m.Version,
		}

		return nil
	})

	close(ch)

	if err != nil {
		tb.Log("error listing blob versions:", err)
	}

	for e := range errChan {
		if e.Err != nil {
			tb.Log("error cleaning up blob versions:", e.Err, e.ObjectName, e.VersionID)
		}
	}
}
