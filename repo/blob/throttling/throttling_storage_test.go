package throttling_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	bloblogging "github.com/kopia/kopia/repo/blob/logging"
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

func (m *mockThrottler) AfterOperation(ctx context.Context, op string) {
	m.activity = append(m.activity, fmt.Sprintf("AfterOperation(%v)", op))
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

func (m *mockThrottler) Printf(msg string, args ...interface{}) {
	msg = fmt.Sprintf(msg, args...)
	msg = strings.Split(msg, "\t")[0] // ignore parameters
	m.activity = append(m.activity, msg)
}

func TestThrottling(t *testing.T) {
	ctx := testlogging.Context(t)
	m := &mockThrottler{}
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	l := bloblogging.NewWrapper(st, testlogging.Printf(m.Printf, ""), "inner.")
	wrapped := throttling.NewWrapper(l, m)

	var tmp gather.WriteBuffer

	m.Reset()
	require.ErrorIs(t, wrapped.GetBlob(ctx, "blob1", 0, -1, &tmp), blob.ErrBlobNotFound)
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)",
		"inner.concurrency level reached",
		"inner.GetBlob",
		"ReturnUnusedDownloadBytes(20000000)",
		"AfterOperation(GetBlob)",
	}, m.activity)

	// upload blob of 7 bytes
	m.Reset()
	require.NoError(t, wrapped.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7}), blob.PutOptions{}))
	require.Equal(t, []string{
		"BeforeOperation(PutBlob)",
		"BeforeUpload(7)",
		"inner.PutBlob",
		"AfterOperation(PutBlob)",
	}, m.activity)

	// upload another blob of 30MB
	m.Reset()
	require.NoError(t, wrapped.PutBlob(ctx, "blob2", gather.FromSlice(bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 3000000)), blob.PutOptions{}))
	require.Equal(t, []string{
		"BeforeOperation(PutBlob)",
		"BeforeUpload(30000000)",
		"inner.PutBlob",
		"AfterOperation(PutBlob)",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob1", 0, -1, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)", // length is unknown, we assume 20MB
		"inner.GetBlob",
		"ReturnUnusedDownloadBytes(19999993)", // refund all but 7 bytes
		"AfterOperation(GetBlob)",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob2", 0, -1, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(20000000)", // length is unknown, we assume 20MB
		"inner.GetBlob",
		"BeforeDownload(10000000)", // we downloaded more than expected, acquire more
		"AfterOperation(GetBlob)",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.GetBlob(ctx, "blob1", 3, 4, &tmp))
	require.Equal(t, []string{
		"BeforeOperation(GetBlob)",
		"BeforeDownload(4)",
		"inner.GetBlob",
		"AfterOperation(GetBlob)",
	}, m.activity)

	m.Reset()

	_, err := wrapped.GetMetadata(ctx, "blob1")
	require.NoError(t, err)
	require.Equal(t, []string{
		"BeforeOperation(GetMetadata)",
		"inner.GetMetadata",
		"AfterOperation(GetMetadata)",
	}, m.activity)

	m.Reset()
	require.NoError(t, wrapped.DeleteBlob(ctx, "blob1"))
	require.Equal(t, []string{
		"BeforeOperation(DeleteBlob)",
		"inner.DeleteBlob",
		"AfterOperation(DeleteBlob)",
	}, m.activity)

	m.Reset()

	_, err = blob.ListAllBlobs(ctx, wrapped, "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"BeforeOperation(ListBlobs)",
		"inner.ListBlobs",
		"AfterOperation(ListBlobs)",
	}, m.activity)
}
