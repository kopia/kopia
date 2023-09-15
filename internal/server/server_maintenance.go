package server

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type srvMaintenance struct {
	triggerChan chan struct{}
	closed      chan struct{}
	cancelCtx   context.CancelFunc
	srv         maintenanceManagerServerInterface
	wg          sync.WaitGroup

	minMaintenanceInterval time.Duration // +checklocksignore

	mu sync.Mutex
	//+checklocks:mu
	cachedNextMaintenanceTime time.Time
	//+checklocks:mu
	nextMaintenanceNoEarlierThan time.Time
}

type maintenanceManagerServerInterface interface {
	runMaintenanceTask(ctx context.Context, dr repo.DirectRepository) error
	refreshScheduler(reason string)
}

func (s *srvMaintenance) trigger() {
	s.beforeRun()

	select {
	case s.triggerChan <- struct{}{}:
	default:
	}
}

func (s *srvMaintenance) stop(ctx context.Context) {
	// cancel context for any running maintenance
	s.cancelCtx()

	// stop the goroutine and wait for it
	close(s.closed)
	s.wg.Wait()

	log(ctx).Debug("maintenance manager stopped")
}

func (s *srvMaintenance) beforeRun() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// make sure we're not scheduling next maintenance until we refresh
	s.cachedNextMaintenanceTime = time.Time{}
}

func (s *srvMaintenance) afterFailedRun() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// make sure we don't run maintenance too often
	s.nextMaintenanceNoEarlierThan = clock.Now().Add(s.minMaintenanceInterval)
}

func (s *srvMaintenance) refresh(ctx context.Context, dr repo.DirectRepository, notify bool) {
	if notify {
		defer s.srv.refreshScheduler("maintenance schedule changed")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.refreshLocked(ctx, dr); err != nil {
		log(ctx).Debugw("unable to refresh maintenance manager", "err", err)
	}
}

func (s *srvMaintenance) refreshLocked(ctx context.Context, dr repo.DirectRepository) error {
	nmt, err := maintenance.TimeToAttemptNextMaintenance(ctx, dr)
	if err != nil {
		return errors.Wrap(err, "unable to get next maintenance time")
	}

	if nmt.Before(s.nextMaintenanceNoEarlierThan) {
		nmt = s.nextMaintenanceNoEarlierThan
	}

	s.cachedNextMaintenanceTime = nmt

	return nil
}

func (s *srvMaintenance) nextMaintenanceTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cachedNextMaintenanceTime
}

func startMaintenanceManager(
	ctx context.Context,
	rep repo.DirectRepository,
	srv maintenanceManagerServerInterface,
	minMaintenanceInterval time.Duration,
) *srvMaintenance {
	mctx, cancel := context.WithCancel(ctx)

	m := srvMaintenance{
		triggerChan:            make(chan struct{}, 1),
		closed:                 make(chan struct{}),
		srv:                    srv,
		cancelCtx:              cancel,
		minMaintenanceInterval: minMaintenanceInterval,
	}

	m.wg.Add(1)

	log(ctx).Debug("starting maintenance manager")

	m.refresh(ctx, rep, false)

	go func() {
		defer m.wg.Done()

		for {
			select {
			case <-m.triggerChan:
				log(ctx).Debug("starting maintenance task")

				m.beforeRun()

				if err := srv.runMaintenanceTask(mctx, rep); err != nil {
					log(ctx).Debugw("maintenance task failed", "err", err)
					m.afterFailedRun()
				}

				m.refresh(mctx, rep, true)

			case <-m.closed:
				log(ctx).Debug("stopping maintenance manager")
				return
			}
		}
	}()

	return &m
}
