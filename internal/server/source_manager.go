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
	statusRefreshInterval       = 30 * time.Minute
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

	server *Server

	src              snapshot.SourceInfo
	rep              repo.Repository
	closed           chan struct{}
	snapshotRequests chan struct{}
	refreshRequested chan struct{} // tickled externally to trigger refresh
	wg               sync.WaitGroup

	sourceMutex sync.RWMutex
	// +checklocks:sourceMutex
	uploader *snapshotfs.Uploader
	// +checklocks:sourceMutex
	pol policy.SchedulingPolicy
	// +checklocks:sourceMutex
	state string
	// +checklocks:sourceMutex
	nextSnapshotTime *time.Time
	// +checklocks:sourceMutex
	lastSnapshot *snapshot.Manifest
	// +checklocks:sourceMutex
	lastCompleteSnapshot *snapshot.Manifest
	// +checklocks:sourceMutex
	manifestsSinceLastCompleteSnapshot []*snapshot.Manifest
	// +checklocks:sourceMutex
	paused bool
	// +checklocks:sourceMutex
	currentTask string

	isReadOnly bool
	progress   *snapshotfs.CountingUploadProgress
}

func (s *sourceManager) Status() *serverapi.SourceStatus {
	s.sourceMutex.RLock()
	defer s.sourceMutex.RUnlock()

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
	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

	s.state = stat
}

func (s *sourceManager) isPaused() bool {
	s.sourceMutex.RLock()
	defer s.sourceMutex.RUnlock()

	return s.paused
}

func (s *sourceManager) getNextSnapshotTime() (time.Time, bool) {
	s.sourceMutex.RLock()
	defer s.sourceMutex.RUnlock()

	if s.nextSnapshotTime == nil {
		return time.Time{}, false
	}

	return *s.nextSnapshotTime, true
}

func (s *sourceManager) setCurrentTaskID(taskID string) {
	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

	s.currentTask = taskID
}

func (s *sourceManager) setNextSnapshotTime(t time.Time) {
	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

	s.nextSnapshotTime = &t
}

func (s *sourceManager) currentUploader() *snapshotfs.Uploader {
	s.sourceMutex.RLock()
	defer s.sourceMutex.RUnlock()

	return s.uploader
}

func (s *sourceManager) setUploader(u *snapshotfs.Uploader) {
	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

	s.uploader = u
}

func (s *sourceManager) start(ctx context.Context, rep repo.Repository) {
	isLocal := rep.ClientOptions().Hostname == s.src.Host && !rep.ClientOptions().ReadOnly

	go s.run(ctx, isLocal)
}

func (s *sourceManager) run(ctx context.Context, isLocal bool) {
	// make sure we run in a detached context, which ignores outside cancelation and deadline.
	ctx = ctxutil.Detach(ctx)

	s.setStatus("INITIALIZING")
	defer s.setStatus("STOPPED")

	s.wg.Add(1)
	defer s.wg.Done()

	if isLocal {
		log(ctx).Debugf("starting local source manager for %v", s.src)
		s.runLocal(ctx)
	} else {
		log(ctx).Debugf("starting read-only source manager for %v", s.src)
		s.runReadOnly(ctx)
	}
}

func (s *sourceManager) runLocal(ctx context.Context) {
	s.refreshStatus(ctx)

	for {
		var waitTime time.Duration

		nst, ok := s.getNextSnapshotTime()
		if ok {
			waitTime = nst.Sub(clock.Now())
		} else {
			waitTime = oneDay
		}

		if s.isPaused() {
			s.setStatus("PAUSED")
		} else {
			s.setStatus("IDLE")
		}

		select {
		case <-s.closed:
			return

		case <-s.snapshotRequests:
			s.setNextSnapshotTime(clock.Now())

			continue

		case <-s.refreshRequested:
			s.refreshStatus(ctx)
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
	if _, ok := s.getNextSnapshotTime(); !ok {
		return
	}

	s.setNextSnapshotTime(clock.Now().Add(failedSnapshotRetryInterval))
}

func (s *sourceManager) runReadOnly(ctx context.Context) {
	s.isReadOnly = true
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
	log(ctx).Debugw("cancel triggered via API", "source", s.src)

	if u := s.currentUploader(); u != nil {
		log(ctx).Infof("canceling current upload")
		u.Cancel()
	}

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) pause(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Debugw("pause triggered via API", "source", s.src)

	s.sourceMutex.Lock()
	s.paused = true
	s.sourceMutex.Unlock()

	if u := s.currentUploader(); u != nil {
		log(ctx).Infof("canceling current upload")
		u.Cancel()
	} else {
		select {
		case s.refreshRequested <- struct{}{}:
		default:
		}
	}

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) resume(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Debugw("resume triggered via API", "source", s.src)

	s.sourceMutex.Lock()
	s.paused = false
	s.sourceMutex.Unlock()

	select {
	case s.refreshRequested <- struct{}{}:
	default:
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

	if !s.server.beginUpload(ctx, s.src) {
		return nil
	}

	defer s.server.endUpload(ctx, s.src)

	// nolint:wrapcheck
	return s.server.taskmgr.Run(ctx,
		"Snapshot",
		fmt.Sprintf("%v at %v", s.src, clock.Now().Format(time.RFC3339)),
		s.snapshotInternal)
}

func (s *sourceManager) snapshotInternal(ctx context.Context, ctrl uitask.Controller) error {
	s.setStatus("UPLOADING")

	s.setCurrentTaskID(ctrl.CurrentTaskID())
	defer s.setCurrentTaskID("")

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

	s.sourceMutex.RLock()
	manifestsSinceLastCompleteSnapshot := append([]*snapshot.Manifest(nil), s.manifestsSinceLastCompleteSnapshot...)
	s.sourceMutex.RUnlock()

	// nolint:wrapcheck
	return repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
		Purpose: "Source Manager Uploader",
		OnUpload: func(numBytes int64) {
			// extra indirection to allow changing onUpload function later
			// once we have the uploader
			onUpload(numBytes)
		},
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
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

		manifest, err := u.Upload(ctx, localEntry, policyTree, s.src, manifestsSinceLastCompleteSnapshot...)
		prog.report(true)

		s.setUploader(nil)

		if err != nil {
			return errors.Wrap(err, "upload error")
		}

		ignoreIdenticalSnapshot := policyTree.EffectivePolicy().RetentionPolicy.IgnoreIdenticalSnapshots.OrDefault(false)
		if ignoreIdenticalSnapshot {
			for _, prev := range manifestsSinceLastCompleteSnapshot {
				if prev.RootObjectID().String() == manifest.RootObjectID().String() {
					return errors.Wrap(err, "Not saving snapshot because no files have been changed")
				}
			}
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

// +checklocksread:s.sourceMutex
func (s *sourceManager) findClosestNextSnapshotTimeReadLocked() *time.Time {
	var lastCompleteSnapshotTime time.Time
	if lcs := s.lastCompleteSnapshot; lcs != nil {
		lastCompleteSnapshotTime = lcs.StartTime
	}

	t, ok := s.pol.NextSnapshotTime(lastCompleteSnapshotTime, clock.Now())
	if !ok {
		return nil
	}

	return &t
}

func (s *sourceManager) refreshStatus(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	pol, _, _, err := policy.GetEffectivePolicy(ctx, s.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	snapshots, err := snapshot.ListSnapshots(ctx, s.rep, s.src)
	if err != nil {
		s.setStatus("FAILED")
		return
	}

	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

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
	} else {
		s.lastSnapshot = nil
	}

	if s.paused {
		s.nextSnapshotTime = nil
	} else {
		s.nextSnapshotTime = s.findClosestNextSnapshotTimeReadLocked()
	}
}

type uitaskProgress struct {
	// +checkatomic
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

func newSourceManager(src snapshot.SourceInfo, server *Server, rep repo.Repository) *sourceManager {
	m := &sourceManager{
		src:              src,
		rep:              rep,
		server:           server,
		state:            "UNKNOWN",
		closed:           make(chan struct{}),
		snapshotRequests: make(chan struct{}, 1),
		refreshRequested: make(chan struct{}, 1),
		progress:         &snapshotfs.CountingUploadProgress{},
	}

	return m
}
