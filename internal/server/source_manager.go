package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
)

const (
	failedSnapshotRetryInterval = 5 * time.Minute
	refreshTimeout              = 30 * time.Second // max amount of time to refresh a single source
	oneDay                      = 24 * time.Hour
)

type sourceManagerServerInterface interface {
	runSnapshotTask(ctx context.Context, src snapshot.SourceInfo, inner func(ctx context.Context, ctrl uitask.Controller) error) error
	refreshScheduler(reason string)
}

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

	server sourceManagerServerInterface

	src              snapshot.SourceInfo
	rep              repo.Repository
	closed           chan struct{}
	snapshotRequests chan struct{}
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
	// +checklocks:sourceMutex
	lastAttemptedSnapshotTime fs.UTCTimestamp

	isReadOnly bool
	progress   *snapshotfs.CountingUploadProgress

	// Metrics
	lastSnapshotStartTime *metrics.Gauge
	lastSnapshotEndTime   *metrics.Gauge
	lastSnapshotSize      *metrics.Gauge
	lastSnapshotFiles     *metrics.Gauge
	lastSnapshotDirs      *metrics.Gauge
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

func (s *sourceManager) start(ctx context.Context, isLocal bool) {
	s.refreshStatus(ctx)
	go s.run(ctx, isLocal)
}

func (s *sourceManager) run(ctx context.Context, isLocal bool) {
	// make sure we run in a detached context, which ignores outside cancellation and deadline.
	ctx = context.WithoutCancel(ctx)

	s.setStatus("INITIALIZING")
	defer s.setStatus("STOPPED")

	s.wg.Add(1)
	defer s.wg.Done()

	if isLocal {
		s.runLocal(ctx)
	} else {
		s.runReadOnly()
	}
}

func (s *sourceManager) runLocal(ctx context.Context) {
	if s.isPaused() {
		s.setStatus("PAUSED")
	} else {
		s.setStatus("IDLE")
	}

	for {
		select {
		case <-s.closed:
			return

		case <-s.snapshotRequests:
			if s.isPaused() {
				s.setStatus("PAUSED")
			} else {
				s.setStatus("PENDING")

				if err := s.server.runSnapshotTask(ctx, s.src, s.snapshotInternal); err != nil {
					log(ctx).Errorf("snapshot error: %v", err)

					s.backoffBeforeNextSnapshot()
				} else {
					s.refreshStatus(ctx)
				}

				s.server.refreshScheduler("snapshot finished")

				s.setStatus("IDLE")
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

func (s *sourceManager) runReadOnly() {
	s.isReadOnly = true
	s.setStatus("REMOTE")

	// wait until closed
	<-s.closed
}

func (s *sourceManager) scheduleSnapshotNow() {
	s.sourceMutex.Lock()
	defer s.sourceMutex.Unlock()

	// next snapshot time will be recalculated by refreshStatus()
	s.nextSnapshotTime = nil

	select {
	case s.snapshotRequests <- struct{}{}: // scheduled snapshot
	default: // already scheduled
	}
}

func (s *sourceManager) upload(ctx context.Context) serverapi.SourceActionResponse {
	s.scheduleSnapshotNow()

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) cancel(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Debugw("cancel triggered via API", "source", s.src)

	if u := s.currentUploader(); u != nil {
		log(ctx).Info("canceling current upload")
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
		log(ctx).Info("canceling current upload")
		u.Cancel()
	}

	s.server.refreshScheduler("source paused")

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) resume(ctx context.Context) serverapi.SourceActionResponse {
	log(ctx).Debugw("resume triggered via API", "source", s.src)

	s.sourceMutex.Lock()
	s.paused = false
	s.sourceMutex.Unlock()

	s.server.refreshScheduler("source unpaused")

	return serverapi.SourceActionResponse{Success: true}
}

func (s *sourceManager) stop(ctx context.Context) {
	if u := s.currentUploader(); u != nil {
		log(ctx).Infow("canceling current upload", "src", s.src)
		u.Cancel()
	}

	close(s.closed)
}

func (s *sourceManager) removeMetrics() {
	if s.rep.Metrics() != nil {
		registry := s.rep.Metrics()
		registry.RemoveGauge(s.lastSnapshotStartTime)
		registry.RemoveGauge(s.lastSnapshotEndTime)
		registry.RemoveGauge(s.lastSnapshotSize)
		registry.RemoveGauge(s.lastSnapshotFiles)
		registry.RemoveGauge(s.lastSnapshotDirs)
	}
}

func (s *sourceManager) waitUntilStopped() {
	s.wg.Wait()
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

	s.sourceMutex.Lock()
	manifestsSinceLastCompleteSnapshot := append([]*snapshot.Manifest(nil), s.manifestsSinceLastCompleteSnapshot...)
	s.lastAttemptedSnapshotTime = fs.UTCTimestampFromTime(clock.Now())
	s.sourceMutex.Unlock()

	//nolint:wrapcheck
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
		prog := &uitaskProgress{
			p:    s.progress,
			ctrl: ctrl,
		}
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
		if ignoreIdenticalSnapshot && len(manifestsSinceLastCompleteSnapshot) > 0 {
			if manifestsSinceLastCompleteSnapshot[0].RootObjectID() == manifest.RootObjectID() {
				log(ctx).Debug("Not saving snapshot because no files have been changed since previous snapshot")
				return nil
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
	var previousSnapshotTime fs.UTCTimestamp
	if lcs := s.lastCompleteSnapshot; lcs != nil {
		previousSnapshotTime = lcs.StartTime
	}

	// consider attempted snapshots even if they did not end up writing snapshot manifests.
	if s.lastAttemptedSnapshotTime.After(previousSnapshotTime) {
		previousSnapshotTime = s.lastAttemptedSnapshotTime
	}

	t, ok := s.pol.NextSnapshotTime(previousSnapshotTime.ToTime(), clock.Now())
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

	// Do not expose metrics if the policy does not allow it.
	if !pol.MetricsPolicy.ExposeMetrics.OrDefault(false) {
		return
	}

	// Update metrics
	if s.lastCompleteSnapshot != nil && s.rep.Metrics() != nil {
		s.lastSnapshotStartTime.Set(s.lastCompleteSnapshot.StartTime.ToTime().Unix())
		s.lastSnapshotEndTime.Set(s.lastCompleteSnapshot.EndTime.ToTime().Unix())
		s.lastSnapshotSize.Set(atomic.LoadInt64(&s.lastCompleteSnapshot.Stats.TotalFileSize))
		s.lastSnapshotFiles.Set(s.lastCompleteSnapshot.RootEntry.DirSummary.TotalFileCount)
		s.lastSnapshotDirs.Set(s.lastCompleteSnapshot.RootEntry.DirSummary.TotalDirCount)
	}
}

type uitaskProgress struct {
	nextReportTimeNanos atomic.Int64
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

	nrt := t.nextReportTimeNanos.Load()
	if n > nrt && t.nextReportTimeNanos.CompareAndSwap(nrt, n+time.Second.Nanoseconds()) {
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

// FinishedFile is emitted when the system is done examining a file.
func (t *uitaskProgress) FinishedFile(fname string, err error) {
	t.p.FinishedFile(fname, err)
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

func newSourceManager(src snapshot.SourceInfo, server sourceManagerServerInterface, rep repo.Repository) *sourceManager {
	m := &sourceManager{
		src:              src,
		rep:              rep,
		server:           server,
		state:            "UNKNOWN",
		closed:           make(chan struct{}),
		snapshotRequests: make(chan struct{}, 1),
		progress:         &snapshotfs.CountingUploadProgress{},
	}

	if rep.Metrics() != nil {
		registry := rep.Metrics()
		m.lastSnapshotStartTime = registry.GaugeInt64("last_snapshot_start_time", "Timestamp of the last snapshot start time", map[string]string{"host": m.src.Host, "username": m.src.UserName, "path": m.src.Path})
		m.lastSnapshotEndTime = registry.GaugeInt64("last_snapshot_end_time", "Timestamp of the last snapshot end time", map[string]string{"host": m.src.Host, "username": m.src.UserName, "path": m.src.Path})
		m.lastSnapshotSize = registry.GaugeInt64("last_snapshot_size", "Size of the last snapshot in bytes", map[string]string{"host": m.src.Host, "username": m.src.UserName, "path": m.src.Path})
		m.lastSnapshotFiles = registry.GaugeInt64("last_snapshot_files", "Number of files in the last snapshot", map[string]string{"host": m.src.Host, "username": m.src.UserName, "path": m.src.Path})
		m.lastSnapshotDirs = registry.GaugeInt64("last_snapshot_dirs", "Number of directories in the last snapshot", map[string]string{"host": m.src.Host, "username": m.src.UserName, "path": m.src.Path})
	}

	return m
}
