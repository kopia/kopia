package server

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	statusRefreshInterval = 15 * time.Second // how frequently to refresh source status
	oneDay                = 24 * time.Hour
)

// sourceManager manages the state machine of each source
// Possible states:
//
// - INITIALIZING - fetching configuration from repository
// - READY - waiting for next snapshot
// - PAUSED - inactive
// - FAILED - inactive
// - UPLOADING - uploading a snapshot
type sourceManager struct {
	snapshotfs.NullUploadProgress

	server           *Server
	src              snapshot.SourceInfo
	closed           chan struct{}
	snapshotRequests chan struct{}
	wg               sync.WaitGroup

	mu                                 sync.RWMutex
	uploader                           *snapshotfs.Uploader
	pol                                policy.SchedulingPolicy
	state                              string
	nextSnapshotTime                   *time.Time
	lastSnapshot                       *snapshot.Manifest
	lastCompleteSnapshot               *snapshot.Manifest
	manifestsSinceLastCompleteSnapshot []*snapshot.Manifest

	progress *snapshotfs.CountingUploadProgress
}

func (s *sourceManager) Status() *serverapi.SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := &serverapi.SourceStatus{
		Source:           s.src,
		Status:           s.state,
		NextSnapshotTime: s.nextSnapshotTime,
		SchedulingPolicy: s.pol,
		LastSnapshot:     s.lastSnapshot,
	}

	if st.Status == "UPLOADING" {
		c := s.progress.Snapshot()

		st.UploadCounters = &c
	}

	return st
}

func (s *sourceManager) setStatus(stat string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = stat
}

func (s *sourceManager) currentUploader() *snapshotfs.Uploader {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.uploader
}

func (s *sourceManager) setUploader(u *snapshotfs.Uploader) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.uploader = u
}

func (s *sourceManager) run(ctx context.Context) {
	s.setStatus("INITIALIZING")
	defer s.setStatus("STOPPED")

	s.wg.Add(1)
	defer s.wg.Done()

	if s.server.rep.Hostname() == s.src.Host {
		log(ctx).Debugf("starting local source manager for %v", s.src)
		s.runLocal(ctx)
	} else {
		log(ctx).Debugf("starting remote source manager for %v", s.src)
		s.runRemote(ctx)
	}
}

func (s *sourceManager) runLocal(ctx context.Context) {
	s.refreshStatus(ctx)

	for {
		var waitTime time.Duration

		if s.nextSnapshotTime != nil {
			waitTime = time.Until(*s.nextSnapshotTime)
			log(ctx).Debugf("time to next snapshot %v is %v", s.src, waitTime)
		} else {
			log(ctx).Debugf("no scheduled snapshot for %v", s.src)
			waitTime = oneDay
		}

		s.setStatus("IDLE")
		select {
		case <-s.closed:
			return

		case <-s.snapshotRequests:
			nt := time.Now()
			s.nextSnapshotTime = &nt

			continue

		case <-time.After(statusRefreshInterval):
			s.refreshStatus(ctx)

		case <-time.After(waitTime):
			log(ctx).Debugf("snapshotting %v", s.src)
			s.snapshot(ctx)
			s.refreshStatus(ctx)
		}
	}
}

func (s *sourceManager) runRemote(ctx context.Context) {
	s.refreshStatus(ctx)
	s.setStatus("REMOTE")

	for {
		select {
		case <-s.closed:
			return
		case <-time.After(statusRefreshInterval):
			s.refreshStatus(ctx)
		}
	}
}

func (s *sourceManager) scheduleSnapshotNow() {
	select {
	case s.snapshotRequests <- struct{}{}: // scheduled snapshot
	default: // already scheduled
	}
}

func (s *sourceManager) upload(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Infof("upload triggered via API: %v", s.src)
	s.scheduleSnapshotNow()

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) cancel(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Infof("cancel triggered via API: %v", s.src)

	if u := s.currentUploader(); u != nil {
		log(ctx).Infof("canceling current upload")
		u.Cancel()
	}

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) stop(ctx context.Context) {
	log(ctx).Debugf("stopping source manager for %v", s.src)

	if u := s.currentUploader(); u != nil {
		log(ctx).Infof("canceling current upload")
		u.Cancel()
	}

	close(s.closed)
}

func (s *sourceManager) waitUntilStopped(ctx context.Context) {
	s.wg.Wait()
	log(ctx).Debugf("source manager for %v has stopped", s.src)
}

func (s *sourceManager) snapshot(ctx context.Context) {
	s.setStatus("PENDING")

	s.server.beginUpload(ctx, s.src)
	defer s.server.endUpload(ctx, s.src)

	s.setStatus("UPLOADING")

	// check if we got closed while waiting on semaphore
	select {
	case <-s.closed:
		log(ctx).Infof("not snapshotting %v because source manager is shutting down", s.src)
		return

	default:
	}

	localEntry, err := localfs.NewEntry(s.src.Path)
	if err != nil {
		log(ctx).Errorf("unable to create local filesystem: %v", err)
		return
	}

	u := snapshotfs.NewUploader(s.server.rep)

	policyTree, err := policy.TreeForSource(ctx, s.server.rep, s.src)
	if err != nil {
		log(ctx).Errorf("unable to create policy getter: %v", err)
	}

	u.Progress = s.progress

	log(ctx).Infof("starting upload of %v", s.src)
	s.setUploader(u)
	manifest, err := u.Upload(ctx, localEntry, policyTree, s.src, s.manifestsSinceLastCompleteSnapshot...)
	s.setUploader(nil)

	if err != nil {
		log(ctx).Errorf("upload error: %v", err)
		return
	}

	snapshotID, err := snapshot.SaveSnapshot(ctx, s.server.rep, manifest)
	if err != nil {
		log(ctx).Errorf("unable to save snapshot: %v", err)
		return
	}

	if _, err := policy.ApplyRetentionPolicy(ctx, s.server.rep, s.src, true); err != nil {
		log(ctx).Errorf("unable to apply retention policy: %v", err)
		return
	}

	log(ctx).Infof("created snapshot %v", snapshotID)

	if err := s.server.rep.Flush(ctx); err != nil {
		log(ctx).Errorf("unable to flush: %v", err)
		return
	}
}

func (s *sourceManager) findClosestNextSnapshotTime() *time.Time {
	var nextSnapshotTime *time.Time

	// compute next snapshot time based on interval
	if interval := s.pol.IntervalSeconds; interval != 0 {
		interval := time.Duration(interval) * time.Second
		nt := s.lastSnapshot.StartTime.Add(interval).Truncate(interval)
		nextSnapshotTime = &nt
	}

	for _, tod := range s.pol.TimesOfDay {
		nowLocalTime := time.Now().Local()
		localSnapshotTime := time.Date(nowLocalTime.Year(), nowLocalTime.Month(), nowLocalTime.Day(), tod.Hour, tod.Minute, 0, 0, time.Local)

		if tod.Hour < nowLocalTime.Hour() || (tod.Hour == nowLocalTime.Hour() && tod.Minute < nowLocalTime.Minute()) {
			localSnapshotTime = localSnapshotTime.Add(oneDay)
		}

		if nextSnapshotTime == nil || localSnapshotTime.Before(*nextSnapshotTime) {
			nextSnapshotTime = &localSnapshotTime
		}
	}

	return nextSnapshotTime
}

func (s *sourceManager) refreshStatus(ctx context.Context) {
	log(ctx).Debugf("refreshing state for %v", s.src)

	pol, _, err := policy.GetEffectivePolicy(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.pol = pol.SchedulingPolicy

	snapshots, err := snapshot.ListSnapshots(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.manifestsSinceLastCompleteSnapshot = nil
	s.lastCompleteSnapshot = nil

	snaps := snapshot.SortByTime(snapshots, true)
	if len(snaps) > 0 {
		s.lastSnapshot = snaps[0]
		for _, sn := range snaps {
			s.manifestsSinceLastCompleteSnapshot = append(s.manifestsSinceLastCompleteSnapshot, sn)

			// complete snapshot, end here
			if sn.IncompleteReason == "" {
				s.lastCompleteSnapshot = sn
				break
			}
		}

		s.nextSnapshotTime = s.findClosestNextSnapshotTime()
	} else {
		s.nextSnapshotTime = nil
		s.lastSnapshot = nil
	}
}

func newSourceManager(src snapshot.SourceInfo, server *Server) *sourceManager {
	m := &sourceManager{
		src:              src,
		server:           server,
		state:            "UNKNOWN",
		closed:           make(chan struct{}),
		snapshotRequests: make(chan struct{}, 1),
		progress:         &snapshotfs.CountingUploadProgress{},
	}

	return m
}
