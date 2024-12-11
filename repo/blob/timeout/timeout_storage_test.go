package timeout_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/timeout"
)

type MockTimeoutTestStorage struct {
	blob.Storage

	returnedContext context.Context //nolint: containedctx
}

func (s *MockTimeoutTestStorage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	s.returnedContext = ctx

	return blob.Metadata{}, nil
}

func TestTimeout(t *testing.T) {
	ms := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	mock := &MockTimeoutTestStorage{Storage: ms}

	// Timeout is greater than 0
	t.Run("With valid timeout", func(t *testing.T) {
		ctx := testlogging.Context(t)

		sw := timeout.NewStorageTimeout(mock, 5)
		_, _ = sw.GetMetadata(ctx, "")

		deadline, ok := mock.returnedContext.Deadline()
		require.True(t, ok, "Expected a deadline to be set")

		expectedDeadline := time.Now().Add(5 * time.Second) //nolint:forbidigo
		require.WithinDuration(t, expectedDeadline, deadline, 1*time.Second, "Deadline should be approximately 5 seconds from now")
	})

	// Timeout is 0, context should be unchanged and cancel should be no-op
	t.Run("With no timeout", func(t *testing.T) {
		ctx := testlogging.Context(t)

		sw := timeout.NewStorageTimeout(mock, 0)
		_, _ = sw.GetMetadata(ctx, "")

		newCtx := mock.returnedContext
		require.Equal(t, ctx, newCtx, "Expected the context to remain unchanged")

		_, ok := ctx.Deadline()
		require.False(t, ok, "Expected no deadline to be set")
	})
}
