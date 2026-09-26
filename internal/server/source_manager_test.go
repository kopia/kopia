package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/upload"
)

type blockingNewWriterRepository struct {
	repo.Repository
}

func (r *blockingNewWriterRepository) NewWriter(ctx context.Context, _ repo.WriteSessionOptions) (context.Context, repo.RepositoryWriter, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

type immediateCancelController struct {
	taskID string
}

func (c *immediateCancelController) CurrentTaskID() string {
	return c.taskID
}

func (c *immediateCancelController) OnCancel(cancelFunc context.CancelFunc) {
	cancelFunc()
}

func (c *immediateCancelController) ReportCounters(map[string]uitask.CounterValue) {}

func (c *immediateCancelController) ReportProgressInfo(string) {}

func TestSourceManagerSnapshotInternalCancelBeforeWriterCreation(t *testing.T) {
	t.Parallel()

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	s := &sourceManager{
		src: snapshot.SourceInfo{
			Host:     "host",
			UserName: "user",
			Path:     testutil.TempDirectory(t),
		},
		rep:      &blockingNewWriterRepository{Repository: env.Repository},
		closed:   make(chan struct{}),
		progress: &upload.CountingUploadProgress{},
	}

	ctrl := &immediateCancelController{taskID: "task-1"}
	result := &notifydata.ManifestWithError{}

	done := make(chan error, 1)

	go func() {
		done <- s.snapshotInternal(ctx, ctrl, result)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "unable to create writer")

	case <-time.After(2 * time.Second):
		t.Fatal("snapshotInternal did not return after task cancellation")
	}
}
