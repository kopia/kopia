package blob_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

func TestListAllBlobs(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	ctx := context.Background()
	st.PutBlob(ctx, "foo", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{})
	st.PutBlob(ctx, "boo", gather.FromSlice([]byte{2, 3, 4}), blob.PutOptions{})
	st.PutBlob(ctx, "bar", gather.FromSlice([]byte{3, 4, 5}), blob.PutOptions{})

	result1, err := blob.ListAllBlobs(ctx, st, "")
	require.NoError(t, err)
	require.ElementsMatch(t, []blob.ID{"foo", "boo", "bar"}, blob.IDsFromMetadata(result1))

	result2, err := blob.ListAllBlobs(ctx, st, "b")
	require.NoError(t, err)
	require.ElementsMatch(t, []blob.ID{"boo", "bar"}, blob.IDsFromMetadata(result2))

	result3, err := blob.ListAllBlobs(ctx, st, "c")
	require.NoError(t, err)
	require.ElementsMatch(t, []blob.ID{}, blob.IDsFromMetadata(result3))
}

func TestIterateAllPrefixesInParallel(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	ctx := context.Background()
	st.PutBlob(ctx, "foo", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{})
	st.PutBlob(ctx, "boo", gather.FromSlice([]byte{2, 3, 4}), blob.PutOptions{})
	st.PutBlob(ctx, "bar", gather.FromSlice([]byte{3, 4, 5}), blob.PutOptions{})

	var (
		mu  sync.Mutex
		got []blob.ID
	)

	require.NoError(t, blob.IterateAllPrefixesInParallel(ctx, 10, st, []blob.ID{
		"b",
		"c",
	}, func(m blob.Metadata) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m.BlobID)

		return nil
	}))

	require.ElementsMatch(t, []blob.ID{"boo", "bar"}, got)

	got = nil

	require.NoError(t, blob.IterateAllPrefixesInParallel(ctx, 0, st, []blob.ID{
		"f",
	}, func(m blob.Metadata) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m.BlobID)

		return nil
	}))

	require.ElementsMatch(t, []blob.ID{"foo"}, got)

	got = nil

	require.NoError(t, blob.IterateAllPrefixesInParallel(ctx, 0, st, []blob.ID{
		"f",
		"b",
	}, func(m blob.Metadata) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m.BlobID)

		return nil
	}))

	require.ElementsMatch(t, []blob.ID{"foo", "bar", "boo"}, got)

	errDummy := errors.New("dummy")

	require.ErrorIs(t, errDummy, blob.IterateAllPrefixesInParallel(ctx, 10, st, []blob.ID{
		"b",
		"c",
	}, func(m blob.Metadata) error {
		return errDummy
	}))
}

func TestEnsureLengthExactly(t *testing.T) {
	require.NoError(t, blob.EnsureLengthExactly(3, 3))
	require.NoError(t, blob.EnsureLengthExactly(3, -1))
	require.Error(t, blob.EnsureLengthExactly(3, 4))
	require.Error(t, blob.EnsureLengthExactly(3, 2))
}

func TestIDsFromMetadata(t *testing.T) {
	require.Equal(t,
		[]blob.ID{"foo", "bar", "baz"},
		blob.IDsFromMetadata([]blob.Metadata{
			{BlobID: "foo", Length: 11},
			{BlobID: "bar", Length: 22},
			{BlobID: "baz", Length: 55},
		}))
}

func TestMaxTimestamp(t *testing.T) {
	t0 := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	t1 := t0.Add(1 * time.Hour)
	t2 := t0.Add(-1 * time.Hour)

	require.Equal(t,
		t1,
		blob.MaxTimestamp([]blob.Metadata{
			{BlobID: "foo", Timestamp: t0},
			{BlobID: "bar", Timestamp: t1},
			{BlobID: "baz", Timestamp: t2},
		}))

	require.Equal(t, time.Time{}, blob.MaxTimestamp([]blob.Metadata{}))
}

func TestMinTimestamp(t *testing.T) {
	t0 := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	t1 := t0.Add(1 * time.Hour)
	t2 := t0.Add(-1 * time.Hour)

	require.Equal(t,
		t2,
		blob.MinTimestamp([]blob.Metadata{
			{BlobID: "foo", Timestamp: t0},
			{BlobID: "bar", Timestamp: t1},
			{BlobID: "baz", Timestamp: t2},
		}))

	require.Equal(t, time.Time{}, blob.MinTimestamp([]blob.Metadata{}))
}

func TestTotalLength(t *testing.T) {
	require.Equal(t,
		int64(357),
		blob.TotalLength([]blob.Metadata{
			{BlobID: "foo", Length: 123},
			{BlobID: "bar", Length: 234},
		}))
}

func TestDeleteMultiple(t *testing.T) {
	data := blobtesting.DataMap{
		"foo": []byte{1, 2, 3},
		"bar": []byte{1, 2, 4},
		"baz": []byte{1, 2, 5},
		"qux": []byte{1, 2, 6},
	}

	st := blobtesting.NewMapStorage(data, nil, nil)

	require.NoError(t, blob.DeleteMultiple(context.Background(), st, []blob.ID{"bar", "qux"}, 4))

	require.Equal(t, blobtesting.DataMap{
		"foo": []byte{1, 2, 3},
		"baz": []byte{1, 2, 5},
	}, data)
}

func TestMetataJSONString(t *testing.T) {
	bm := blob.Metadata{
		BlobID:    "foo",
		Length:    12345,
		Timestamp: time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
	}

	require.JSONEq(t, `{"id":"foo","length":12345,"timestamp":"2000-01-02T03:04:05.000000006Z"}`, bm.String())
}

func TestPutBlobAndGetMetadata(t *testing.T) {
	data := blobtesting.DataMap{}

	fixedTime := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)

	st := blobtesting.NewMapStorage(data, nil, func() time.Time {
		return fixedTime
	})

	bm, err := blob.PutBlobAndGetMetadata(context.Background(), st, "foo", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{})
	require.NoError(t, err)
	require.Equal(t, fixedTime, bm.Timestamp)
}
