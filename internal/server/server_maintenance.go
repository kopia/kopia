package server

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/notification/notifydata"
	"github.com/kopia/kopia/notification/notifytemplate"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type srvMaintenance struct {
	triggerChan chan struct{}
	closed      chan struct{}
	cancelCtx   context.CancelFunc
	srv         maintenanceManagerServerInterface
	wg          sync.WaitGroup
	dr          repo.DirectRepository

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
	enableErrorNotifications() bool
	notificationTemplateOptions() notifytemplate.Options
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

func (s *srvMaintenance) refresh(ctx context.Context, notify bool) {
	if notify {
		defer s.srv.refreshScheduler("maintenance schedule changed")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.refreshLocked(ctx); err != nil {
		log(ctx).Debugw("unable to refresh maintenance manager", "err", err)
	}
}

func (s *srvMaintenance) refreshLocked(ctx context.Context) error {
	nmt, err := maintenance.TimeToAttemptNextMaintenance(ctx, s.dr)
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
		dr:                     rep,
	}

	m.wg.Add(1)

	log(ctx).Debug("starting maintenance manager")

	m.refresh(ctx, false)

	go func() {
		defer m.wg.Done()

		for {
			select {
			case <-m.triggerChan:
				log(ctx).Debug("starting maintenance task")

				m.beforeRun()

				t0 := clock.Now()

				if err := srv.runMaintenanceTask(mctx, rep); err != nil {
					log(ctx).Debugw("maintenance task failed", "err", err)
					m.afterFailedRun()

					if srv.enableErrorNotifications() {
						notification.Send(ctx,
							rep,
							"generic-error",
							notifydata.NewErrorInfo("Maintenance", "Scheduled Maintenance", t0, clock.Now(), err),
							notification.SeverityError,
							srv.notificationTemplateOptions(),
						)
					}
				}

				m.refresh(mctx, true)

			case <-m.closed:
				log(ctx).Debug("stopping maintenance manager")
				return
			}
		}
	}()

	return &m
}
