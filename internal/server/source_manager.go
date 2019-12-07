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

// sourceManager manages the state machine of each source
// Possible states:
//
// - INITIALIZING - fetching configuration from repository
// - READY - waiting for next snapshot
// - PAUSED - inactive
// - FAILED - inactive
// - UPLOADING - uploading a snapshot
type sourceManager struct {
	server *Server
	src    snapshot.SourceInfo
	closed chan struct{}

	mu                   sync.RWMutex
	pol                  *policy.Policy
	state                string
	nextSnapshotTime     time.Time
	lastCompleteSnapshot *snapshot.Manifest
	lastSnapshot         *snapshot.Manifest

	// state of current upload
	uploadPath          string
	uploadPathCompleted int64
	uploadPathTotal     int64
}

func (s *sourceManager) Status() *serverapi.SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := &serverapi.SourceStatus{
		Source:           s.src,
		Status:           s.state,
		LastSnapshotSize: s.lastSnapshot.Stats.TotalFileSize,
		LastSnapshotTime: s.lastSnapshot.StartTime,
		NextSnapshotTime: s.nextSnapshotTime,
		Policy:           s.pol,
	}

	st.UploadStatus.UploadingPath = s.uploadPath
	st.UploadStatus.UploadingPathCompleted = s.uploadPathCompleted
	st.UploadStatus.UploadingPathTotal = s.uploadPathTotal

	return st
}

func (s *sourceManager) setStatus(stat string) {
	s.mu.Lock()
	s.state = stat
	s.mu.Unlock()
}

func (s *sourceManager) run(ctx context.Context) {
	s.setStatus("INITIALIZING")
	defer s.setStatus("STOPPED")

	if s.server.hostname == s.src.Host {
		s.runLocal(ctx)
	} else {
		s.runRemote(ctx)
	}
}

func (s *sourceManager) runLocal(ctx context.Context) {
	s.refreshStatus(ctx)

	for {
		var timeBeforeNextSnapshot time.Duration

		if !s.nextSnapshotTime.IsZero() {
			timeBeforeNextSnapshot = time.Until(s.nextSnapshotTime)
			log.Infof("time to next snapshot %v is %v", s.src, timeBeforeNextSnapshot)
		} else {
			timeBeforeNextSnapshot = 24 * time.Hour
		}

		s.setStatus("WAITING")
		select {
		case <-s.closed:
			return

		case <-time.After(15 * time.Second):
			s.refreshStatus(ctx)

		case <-time.After(timeBeforeNextSnapshot):
			log.Infof("snapshotting %v", s.src)
			s.setStatus("SNAPSHOTTING")
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
		case <-time.After(15 * time.Second):
			s.refreshStatus(ctx)
		}
	}
}

func (s *sourceManager) Progress(path string, numFiles int, pathCompleted, pathTotal int64, stats *snapshot.Stats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.uploadPath = path
	s.uploadPathCompleted = pathCompleted
	s.uploadPathTotal = pathTotal
	log.Debugf("path: %v %v/%v", path, pathCompleted, pathTotal)
}

func (s *sourceManager) UploadFinished() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.uploadPath = ""
	s.uploadPathCompleted = 0
	s.uploadPathTotal = 0
}

func (s *sourceManager) upload() serverapi.SourceActionResponse {
	log.Infof("upload triggered via API: %v", s.src)
	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) cancel() serverapi.SourceActionResponse {
	log.Infof("cancel triggered via API: %v", s.src)
	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) pause() serverapi.SourceActionResponse {
	log.Infof("pause triggered via API: %v", s.src)
	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) resume() serverapi.SourceActionResponse {
	log.Infof("resume triggered via API: %v", s.src)
	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) snapshot(ctx context.Context) {
	s.server.beginUpload(s.src)
	defer s.server.endUpload(s.src)

	localEntry, err := localfs.NewEntry(s.src.Path)
	if err != nil {
		log.Errorf("unable to create local filesystem: %v", err)
		return
	}

	u := snapshotfs.NewUploader(s.server.rep)

	policyTree, err := policy.TreeForSource(ctx, s.server.rep, s.src)
	if err != nil {
		log.Errorf("unable to create policy getter: %v", err)
	}

	u.Progress = s

	log.Infof("starting upload of %v", s.src)

	manifest, err := u.Upload(ctx, localEntry, policyTree, s.src, s.lastCompleteSnapshot, s.lastSnapshot)
	if err != nil {
		log.Errorf("upload error: %v", err)
		return
	}

	snapshotID, err := snapshot.SaveSnapshot(ctx, s.server.rep, manifest)
	if err != nil {
		log.Errorf("unable to save snapshot: %v", err)
		return
	}

	if _, err := policy.ApplyRetentionPolicy(ctx, s.server.rep, s.src, true); err != nil {
		log.Errorf("unable to apply retention policy: %v", err)
		return
	}

	log.Infof("created snapshot %v", snapshotID)

	if err := s.server.rep.Flush(ctx); err != nil {
		log.Errorf("unable to flush: %v", err)
		return
	}
}

func (s *sourceManager) findClosestNextSnapshotTime() time.Time {
	nextSnapshotTime := time.Now().Add(24 * time.Hour)

	if s.pol != nil {
		// compute next snapshot time based on interval
		if interval := s.pol.SchedulingPolicy.IntervalSeconds; interval != 0 {
			interval := time.Duration(interval) * time.Second
			nt := s.lastSnapshot.StartTime.Add(interval).Truncate(interval)

			if nt.Before(nextSnapshotTime) {
				nextSnapshotTime = nt
			}
		}

		for _, tod := range s.pol.SchedulingPolicy.TimesOfDay {
			nowLocalTime := time.Now().Local()
			localSnapshotTime := time.Date(nowLocalTime.Year(), nowLocalTime.Month(), nowLocalTime.Day(), tod.Hour, tod.Minute, 0, 0, time.Local)

			if tod.Hour < nowLocalTime.Hour() || (tod.Hour == nowLocalTime.Hour() && tod.Minute < nowLocalTime.Minute()) {
				localSnapshotTime = localSnapshotTime.Add(24 * time.Hour)
			}

			if localSnapshotTime.Before(nextSnapshotTime) {
				nextSnapshotTime = localSnapshotTime
			}
		}
	}

	return nextSnapshotTime
}

func (s *sourceManager) refreshStatus(ctx context.Context) {
	log.Debugf("refreshing state for %v", s.src)

	pol, _, err := policy.GetEffectivePolicy(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.pol = pol

	snapshots, err := snapshot.ListSnapshots(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.lastCompleteSnapshot = nil

	snaps := snapshot.SortByTime(snapshots, true)
	if len(snaps) > 0 {
		s.lastSnapshot = snaps[0]
		s.nextSnapshotTime = s.findClosestNextSnapshotTime()
	} else {
		s.nextSnapshotTime = time.Time{}
		s.lastSnapshot = nil
	}
}

func newSourceManager(src snapshot.SourceInfo, server *Server) *sourceManager {
	m := &sourceManager{
		src:    src,
		server: server,
		state:  "UNKNOWN",
		closed: make(chan struct{}),
	}

	return m
}
