package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	statusRefreshInterval       = 15 * time.Second // how frequently to refresh source status
	failedSnapshotRetryInterval = 5 * time.Minute
	refreshTimeout              = 30 * time.Second // max amount of time to refresh a single source
	oneDay                      = 24 * time.Hour
)

// sourceManager manages the state machine of each source
// Possible states:
//
// - INITIALIZING - fetching configuration from repository
// - READY - waiting for next snapshot
// - PAUSED - inactive
// - FAILED - inactive
// - UPLOADING - uploading a snapshot.
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

	progress    *snapshotfs.CountingUploadProgress
	currentTask string
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

		st.CurrentTask = s.currentTask
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
	// make sure we run in a detached context, which ignores outside cancelation and deadline.
	ctx = ctxutil.Detach(ctx)

	s.setStatus("INITIALIZING")
	defer s.setStatus("STOPPED")

	s.wg.Add(1)
	defer s.wg.Done()

	if s.server.rep.ClientOptions().Hostname == s.src.Host {
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
			waitTime = clock.Until(*s.nextSnapshotTime)
		} else {
			waitTime = oneDay
		}

		s.setStatus("IDLE")
		select {
		case <-s.closed:
			return

		case <-s.snapshotRequests:
			nt := clock.Now()
			s.nextSnapshotTime = &nt

			continue

		case <-time.After(statusRefreshInterval):
			s.refreshStatus(ctx)

		case <-time.After(waitTime):
			log(ctx).Debugf("snapshotting %v", s.src)

			if err := s.snapshot(ctx); err != nil {
				log(ctx).Errorf("snapshot error: %v", err)

				s.backoffBeforeNextSnapshot()
			} else {
				s.refreshStatus(ctx)
			}
		}
	}
}

func (s *sourceManager) backoffBeforeNextSnapshot() {
	if s.nextSnapshotTime == nil {
		return
	}

	t := clock.Now().Add(failedSnapshotRetryInterval)
	s.nextSnapshotTime = &t
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

func (s *sourceManager) snapshot(ctx context.Context) error {
	s.setStatus("PENDING")

	s.server.beginUpload(ctx, s.src)
	defer s.server.endUpload(ctx, s.src)

	return s.server.taskmgr.Run(ctx,
		"Snapshot",
		fmt.Sprintf("%v at %v", s.src, clock.Now().Format(time.RFC3339)),
		s.snapshotInternal)
}

func (s *sourceManager) snapshotInternal(ctx context.Context, ctrl uitask.Controller) error {
	s.setStatus("UPLOADING")

	s.currentTask = ctrl.CurrentTaskID()

	defer func() { s.currentTask = "" }()

	// check if we got closed while waiting on semaphore
	select {
	case <-s.closed:
		log(ctx).Infof("not snapshotting %v because source manager is shutting down", s.src)
		return nil

	default:
	}

	localEntry, err := localfs.NewEntry(s.src.Path)
	if err != nil {
		return errors.Wrap(err, "unable to create local filesystem")
	}

	onUpload := func(int64) {}

	return repo.WriteSession(ctx, s.server.rep, repo.WriteSessionOptions{
		Purpose: "Source Manager Uploader",
		OnUpload: func(numBytes int64) {
			// extra indirection to allow changing onUpload function later
			// once we have the uploader
			onUpload(numBytes)
		},
	}, func(w repo.RepositoryWriter) error {
		log(ctx).Debugf("uploading %v", s.src)
		u := snapshotfs.NewUploader(w)

		ctrl.OnCancel(u.Cancel)

		policyTree, err := policy.TreeForSource(ctx, w, s.src)
		if err != nil {
			return errors.Wrap(err, "unable to create policy getter")
		}

		// set up progress that will keep counters and report to the uitask.
		prog := &uitaskProgress{0, s.progress, ctrl}
		u.Progress = prog
		onUpload = func(numBytes int64) {
			u.Progress.UploadedBytes(numBytes)
		}

		log(ctx).Debugf("starting upload of %v", s.src)
		s.setUploader(u)

		manifest, err := u.Upload(ctx, localEntry, policyTree, s.src, nil, s.manifestsSinceLastCompleteSnapshot...)
		prog.report(true)

		s.setUploader(nil)

		if err != nil {
			return errors.Wrap(err, "upload error")
		}

		snapshotID, err := snapshot.SaveSnapshot(ctx, w, manifest)
		if err != nil {
			return errors.Wrap(err, "unable to save snapshot")
		}

		if _, err := policy.ApplyRetentionPolicy(ctx, w, s.src, true); err != nil {
			return errors.Wrap(err, "unable to apply retention policy")
		}

		log(ctx).Debugf("created snapshot %v", snapshotID)
		return nil
	})
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
		nowLocalTime := clock.Now().Local()
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
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	pol, _, err := policy.GetEffectivePolicy(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	snapshots, err := snapshot.ListSnapshots(ctx, s.server.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pol = pol.SchedulingPolicy
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

type uitaskProgress struct {
	nextReportTimeNanos int64 // must be aligned due to atomic access
	p                   *snapshotfs.CountingUploadProgress
	ctrl                uitask.Controller
}

// report reports the current progress to UITask.
func (t *uitaskProgress) report(final bool) {
	t.ctrl.ReportCounters(t.p.UITaskCounters(final))
}

// maybeReport occasionally reports current progress to UI task.
func (t *uitaskProgress) maybeReport() {
	n := clock.Now().UnixNano()

	nrt := atomic.LoadInt64(&t.nextReportTimeNanos)
	if n > nrt && atomic.CompareAndSwapInt64(&t.nextReportTimeNanos, nrt, n+time.Second.Nanoseconds()) {
		t.report(false)
	}
}

// UploadStarted is emitted once at the start of an upload.
func (t *uitaskProgress) UploadStarted() {
	t.p.UploadStarted()
	t.maybeReport()
}

// UploadFinished is emitted once at the end of an upload.
func (t *uitaskProgress) UploadFinished() {
	t.p.UploadFinished()
	t.maybeReport()
}

// CachedFile is emitted whenever uploader reuses previously uploaded entry without hashing the file.
func (t *uitaskProgress) CachedFile(path string, size int64) {
	t.p.CachedFile(path, size)
	t.maybeReport()
}

// HashingFile is emitted at the beginning of hashing of a given file.
func (t *uitaskProgress) HashingFile(fname string) {
	t.p.HashingFile(fname)
	t.maybeReport()
}

// FinishedHashingFile is emitted at the end of hashing of a given file.
func (t *uitaskProgress) FinishedHashingFile(fname string, numBytes int64) {
	t.p.FinishedHashingFile(fname, numBytes)
	t.maybeReport()
}

// HashedBytes is emitted while hashing any blocks of bytes.
func (t *uitaskProgress) HashedBytes(numBytes int64) {
	t.p.HashedBytes(numBytes)
	t.maybeReport()
}

// Error is emitted when an error is encountered.
func (t *uitaskProgress) Error(path string, err error, isIgnored bool) {
	t.p.Error(path, err, isIgnored)
	t.maybeReport()
}

// UploadedBytes is emitted whenever bytes are written to the blob storage.
func (t *uitaskProgress) UploadedBytes(numBytes int64) {
	t.p.UploadedBytes(numBytes)
	t.maybeReport()
}

// StartedDirectory is emitted whenever a directory starts being uploaded.
func (t *uitaskProgress) StartedDirectory(dirname string) {
	t.p.StartedDirectory(dirname)
	t.maybeReport()

	t.ctrl.ReportProgressInfo(dirname)
}

// FinishedDirectory is emitted whenever a directory is finished uploading.
func (t *uitaskProgress) FinishedDirectory(dirname string) {
	t.p.FinishedDirectory(dirname)
	t.maybeReport()
}

// ExcludedFile is emitted whenever a file is excluded.
func (t *uitaskProgress) ExcludedFile(fname string, numBytes int64) {
	t.p.ExcludedFile(fname, numBytes)
	t.maybeReport()
}

// ExcludedDir is emitted whenever a directory is excluded.
func (t *uitaskProgress) ExcludedDir(dirname string) {
	t.p.ExcludedDir(dirname)
	t.maybeReport()
}

// EstimatedDataSize is emitted whenever the size of upload is estimated.
func (t *uitaskProgress) EstimatedDataSize(fileCount int, totalBytes int64) {
	t.p.EstimatedDataSize(fileCount, totalBytes)
	t.maybeReport()
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
