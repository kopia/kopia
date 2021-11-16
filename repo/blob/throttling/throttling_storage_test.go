package throttling_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type mockThrottler struct {
	activity []string
}

func (m *mockThrottler) Reset() {
	m.activity = nil
}

func (m *mockThrottler) BeforeOperation(ctx context.Context, op string) {
	m.activity = append(m.activity, fmt.Sprintf("BeforeOperation(%v)", op))
}

func (m *mockThrottler) BeforeDownload(ctx context.Context, numBytes int64) {
	m.activity = append(m.activity, fmt.Sprintf("BeforeDownload(%v)", numBytes))
}

func (m *mockThrottler) BeforeUpload(ctx context.Context, numBytes int64) {
	m.activity = append(m.activity, fmt.Sprintf("BeforeUpload(%v)", numBytes))
}

func (m *mockThrottler) ReturnUnusedDownloadBytes(ctx context.Context, numBytes int64) {
	m.activity = append(m.activity, fmt.Sprintf("ReturnUnusedDownloadBytes(%v)", numBytes))
}

func (m *mockThrottler) Debugw(msg string, args ...interface{}) {
	m.activity = append(m.activity, msg)
}

func (m *mockThrottler) Debugf(msg string, args ...interface{}) {}
func (m *mockThrottler) Infof(msg string, args ...interface{})  {}
func (m *mockThrottler) Warnf(msg string, args ...interface{})  {}
func (m *mockThrottler) Errorf(msg string, args ...interface{}) {}

func TestThrottling(t *testing.T) {
	ctx := testlogging.Context(t)
	m := &mockThrottler{}
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	l := logging.NewWrapper(st, m, "inner.")
	wrapped := throttling.NewWrapper(l, m)

	var tmp gather.WriteBuffer

	m.Reset()
	require.ErrorIs(t, wrapped.GetBlob(ctx, "blob1", 0, -1, &tmp), blob.ErrBlobNotFound)
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)",
		"inner.GetBlob",
		"ReturnUnusedDownloadBytes(20000000)",
	}, m.activity)

	// upload blob of 7 bytes
	m.Reset()
	require.NoError(t, wrapped.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7}), blob.PutOptions{}))
	require.Equal(t, []string{
		"BeforeOperation(PutBlob)",
		"BeforeUpload(7)",
		"inner.PutBlob",
	}, m.activity)

	// upload another blob of 30MB
	m.Reset()
	require.NoError(t, wrapped.PutBlob(ctx, "blob2", gather.FromSlice(bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 3000000)), blob.PutOptions{}))
	require.Equal(t, []string{
		"BeforeOperation(PutBlob)",
		"BeforeUpload(30000000)",
		"inner.PutBlob",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob1", 0, -1, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)", // length is unknown, we assume 20MB
		"inner.GetBlob",
		"ReturnUnusedDownloadBytes(19999993)", // refund all but 7 bytes
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob2", 0, -1, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)", // length is unknown, we assume 20MB
		"inner.GetBlob",
		"BeforeDownload(10000000)", // we downloaded more than expected, acquire more
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob1", 3, 4, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(4)",
		"inner.GetBlob",
	}, m.activity)

	m.Reset()

	_, err := wrapped.GetMetadata(ctx, "blob1")
	require.NoError(t, err, blob.ErrBlobNotFound)
	require.Equal(t, []string{
		"BeforeOperation(GetMetadata)",
		"inner.GetMetadata",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.SetTime(ctx, "blob1", clock.Now()))
	require.Equal(t, []string{
		"BeforeOperation(SetTime)",
		"inner.SetTime",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.DeleteBlob(ctx, "blob1"))
	require.Equal(t, []string{
		"BeforeOperation(DeleteBlob)",
		"inner.DeleteBlob",
	}, m.activity)

	m.Reset()

	_, err = blob.ListAllBlobs(ctx, wrapped, "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"BeforeOperation(ListBlobs)",
		"inner.ListBlobs",
	}, m.activity)
}
