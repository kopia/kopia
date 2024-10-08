package storagemetrics_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/storagemetrics"
)

func TestStorageMetrics_PutBlob(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_upload_bytes", 0)
	requireCounterValue(t, snap, "blob_errors[method:PutBlob]", 0)

	require.ErrorIs(t, ms.PutBlob(ctx, "someBlob1", gather.FromSlice([]byte{1, 2, 3, 4}), blob.PutOptions{}), someError)
	require.NoError(t, ms.PutBlob(ctx, "someBlob", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:PutBlob]", 1)
	requireCounterValue(t, snap, "blob_upload_bytes", 3)

	d := snap.DurationDistributions["blob_storage_latency[method:PutBlob]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_GetBlob(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	require.NoError(t, st.PutBlob(ctx, "someBlob", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodGetBlob).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_download_full_blob_bytes", 0)
	requireCounterValue(t, snap, "blob_download_partial_blob_bytes", 0)
	requireCounterValue(t, snap, "blob_errors[method:GetBlob]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.ErrorIs(t, ms.GetBlob(ctx, "someBlob", 0, -1, &tmp), someError)
	require.NoError(t, ms.GetBlob(ctx, "someBlob", 0, -1, &tmp))

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:GetBlob]", 1)
	requireCounterValue(t, snap, "blob_download_full_blob_bytes", 5)

	d := snap.DurationDistributions["blob_storage_latency[method:GetBlob-full]"]
	require.EqualValues(t, 2, d.Count)

	require.NoError(t, ms.GetBlob(ctx, "someBlob", 2, 2, &tmp))

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:GetBlob]", 1)
	requireCounterValue(t, snap, "blob_download_full_blob_bytes", 5)
	require.Equal(t, 2, tmp.Length())

	d = snap.DurationDistributions["blob_storage_latency[method:GetBlob-partial]"]
	require.EqualValues(t, 1, d.Count)
}

func TestStorageMetrics_GetMetadata(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	require.NoError(t, st.PutBlob(ctx, "someBlob", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodGetMetadata).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:GetMetadata]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	_, err := ms.GetMetadata(ctx, "someBlob")
	require.ErrorIs(t, err, someError)

	_, err = ms.GetMetadata(ctx, "someBlob")
	require.NoError(t, err)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:GetMetadata]", 1)

	d := snap.DurationDistributions["blob_storage_latency[method:GetMetadata]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_GetCapacity(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodGetCapacity).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:GetCapacity]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	_, err := ms.GetCapacity(ctx)
	require.ErrorIs(t, err, someError)

	_, err = ms.GetCapacity(ctx)
	require.ErrorIs(t, err, blob.ErrNotAVolume)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:GetCapacity]", 2)

	d := snap.DurationDistributions["blob_storage_latency[method:GetCapacity]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_DeleteBlob(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	require.NoError(t, st.PutBlob(ctx, "someBlob", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:DeleteBlob]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := ms.DeleteBlob(ctx, "someBlob")
	require.ErrorIs(t, err, someError)

	err = ms.DeleteBlob(ctx, "someBlob")
	require.NoError(t, err)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:DeleteBlob]", 1)

	d := snap.DurationDistributions["blob_storage_latency[method:DeleteBlob]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_Close(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodClose).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:Close]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := ms.Close(ctx)
	require.ErrorIs(t, err, someError)

	err = ms.Close(ctx)
	require.NoError(t, err)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:Close]", 1)

	d := snap.DurationDistributions["blob_storage_latency[method:Close]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_FlushCaches(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodFlushCaches).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:FlushCaches]", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := ms.FlushCaches(ctx)
	require.ErrorIs(t, err, someError)

	err = ms.FlushCaches(ctx)
	require.NoError(t, err)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:FlushCaches]", 1)

	d := snap.DurationDistributions["blob_storage_latency[method:FlushCaches]"]
	require.EqualValues(t, 2, d.Count)
}

func TestStorageMetrics_ListBlobs(t *testing.T) {
	ctx := testlogging.Context(t)
	someError := errors.New("foo")
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	require.NoError(t, st.PutBlob(ctx, "someBlob1", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))
	require.NoError(t, st.PutBlob(ctx, "someBlob2", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))
	require.NoError(t, st.PutBlob(ctx, "someBlob3", gather.FromSlice([]byte{1, 2, 3, 4, 5}), blob.PutOptions{}))

	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodListBlobs).ErrorInstead(someError)

	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(fs, mr)
	snap := mr.Snapshot(false)

	requireCounterValue(t, snap, "blob_errors[method:ListBlobs]", 0)
	requireCounterValue(t, snap, "blob_list_items", 0)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := ms.ListBlobs(ctx, "", func(bm blob.Metadata) error { return nil })
	require.ErrorIs(t, err, someError)

	err = ms.ListBlobs(ctx, "", func(bm blob.Metadata) error { return nil })
	require.NoError(t, err)

	snap = mr.Snapshot(false)
	requireCounterValue(t, snap, "blob_errors[method:ListBlobs]", 1)

	d := snap.DurationDistributions["blob_storage_latency[method:ListBlobs]"]
	require.EqualValues(t, 2, d.Count)
	requireCounterValue(t, snap, "blob_list_items", 3)
}

func TestStorageMetrics_Misc(t *testing.T) {
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	mr := metrics.NewRegistry()
	ms := storagemetrics.NewWrapper(st, mr)
	require.Equal(t, st.ConnectionInfo(), ms.ConnectionInfo())
	require.Equal(t, st.DisplayName(), ms.DisplayName())
}

func requireCounterValue(t *testing.T, snap metrics.Snapshot, key string, want int64) {
	t.Helper()

	v, ok := snap.Counters[key]
	require.True(t, ok)
	require.EqualValues(t, want, v)
}
