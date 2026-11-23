package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// blockingSnapshotServer is a sourceManagerServerInterface whose runSnapshotTask blocks
// until released. It simulates a snapshot that has been dequeued by the source manager
// but is waiting for a parallel upload slot (Server.beginUpload).
type blockingSnapshotServer struct {
	entered  chan struct{}
	release  chan struct{}
	runCount atomic.Int32
}

func (s *blockingSnapshotServer) runSnapshotTask(
	_ context.Context,
	_ snapshot.SourceInfo,
	_ func(ctx context.Context, ctrl uitask.Controller, result *notifydata.ManifestWithError) error,
) error {
	s.runCount.Add(1)

	select {
	case s.entered <- struct{}{}:
	default:
	}

	<-s.release

	return nil
}

func (s *blockingSnapshotServer) refreshScheduler(_ string) {}

// TestSourceManager_refreshStatus_whileSnapshotPending verifies that refreshing a
// source's status while its snapshot is queued (waiting for an upload slot) does not
// compute an overdue next snapshot time, which would make the scheduler queue the same
// snapshot a second time and run it back-to-back.
func TestSourceManager_refreshStatus_whileSnapshotPending(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	si := env.LocalPathSourceInfo(testutil.TempDirectory(t))

	require.NoError(t, policy.SetPolicy(ctx, env.RepositoryWriter, si, &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			IntervalSeconds: int64(time.Hour / time.Second),
		},
	}))
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	srv := &blockingSnapshotServer{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}

	sm := newSourceManager(si, srv, env.RepositoryWriter)
	sm.start(ctx, true)

	// The scheduler fires: the snapshot is queued and blocks waiting for an upload slot.
	sm.scheduleSnapshotNow()

	select {
	case <-srv.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot task was not started")
	}

	// The periodic source refresh (Server.syncSourcesLocked) runs while the snapshot is
	// still pending.
	sm.refreshStatus(ctx)

	if nst, ok := sm.getNextSnapshotTime(); ok {
		require.True(t, nst.After(clock.Now()),
			"pending source was rescheduled as overdue (next snapshot time %v)", nst)
	}

	close(srv.release)
	sm.stop(ctx)
	sm.waitUntilStopped()

	require.Equal(t, int32(1), srv.runCount.Load())
}
