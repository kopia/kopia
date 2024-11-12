package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type testServer struct {
	mu sync.Mutex

	runCounter            atomic.Int32
	refreshSchedulerCount atomic.Int32

	log func(msg string, args ...any)

	// +checklocks:mu
	err error
}

func (s *testServer) runMaintenanceTask(ctx context.Context, dr repo.DirectRepository) error {
	s.runCounter.Add(1)

	if s.log != nil {
		s.log("runMaintenanceTask")
	}

	s.mu.Lock()
	ne := s.err
	s.err = nil
	s.mu.Unlock()

	return ne
}

func (s *testServer) refreshScheduler(reason string) {
	s.refreshSchedulerCount.Add(1)
}

func (s *testServer) enableErrorNotifications() bool {
	return false
}

func (s *testServer) notificationTemplateOptions() notifytemplate.Options {
	return notifytemplate.DefaultOptions
}

func TestServerMaintenance(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	require.NoError(t, repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, dw repo.DirectRepositoryWriter) error {
		return maintenance.SetParams(ctx, dw, &maintenance.Params{
			Owner: env.Repository.ClientOptions().UsernameAtHost(),
			QuickCycle: maintenance.CycleParams{
				Enabled:  true,
				Interval: 5 * time.Second,
			},
			FullCycle: maintenance.CycleParams{
				Enabled:  true,
				Interval: 10 * time.Second,
			},
		})
	}))

	ts := &testServer{log: t.Logf}

	mm := startMaintenanceManager(ctx, env.RepositoryWriter, ts, time.Minute)
	require.Equal(t, time.Time{}, mm.nextMaintenanceNoEarlierThan)

	defer mm.stop(ctx)

	// trigger and make sure it runs
	mm.trigger()
	require.Eventually(t, func() bool {
		return ts.runCounter.Load() == 1 && ts.refreshSchedulerCount.Load() == 1
	}, 3*time.Second, 10*time.Millisecond)

	ts.mu.Lock()
	ts.err = errors.New("some error")
	ts.mu.Unlock()

	mm.trigger()

	require.Eventually(t, func() bool {
		mm.mu.Lock()
		defer mm.mu.Unlock()

		return ts.runCounter.Load() == 2 && !mm.nextMaintenanceNoEarlierThan.IsZero()
	}, 3*time.Second, 10*time.Millisecond)

	// after a failure next maintenance time should be deferred by a minute.
	require.Greater(t, mm.nextMaintenanceTime().Sub(clock.Now()), 50*time.Second)
}
