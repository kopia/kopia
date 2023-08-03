package snapshotfs

import (
	"context"
	"io"
	"math/rand"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/workshare"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	scannerLog = logging.Module("scanner")
)

type fileHistogram struct {
	totalSymlink     uint32
	totalFiles       uint32
	size0Byte        uint32
	size0bTo100Kb    uint32
	size100KbTo100Mb uint32
	size100MbTo1Gb   uint32
	sizeOver1Gb      uint32
}

type dirHistogram struct {
	totalDirs             uint32
	numEntries0           uint32
	numEntries0to100      uint32
	numEntries100to1000   uint32
	numEntries1000to10000 uint32
	numEntries10000to1mil uint32
	numEntriesOver1mil    uint32
}

type sourceHistogram struct {
	totalSize  uint64
	errorCount uint32
	files      fileHistogram
	dirs       dirHistogram
}

type scanWorkItem struct {
	err error
}

// Scanner supports efficient uploading files and directories to repository.
type Scanner struct {
	// TODO: we are repurposing the existing progress tracker at the moment,
	// but maybe we should rename the UploadProgress to ScanProgress
	Progress UploadProgress

	// probability with cached entries will be ignored, must be [0..100]
	// 0=always use cached object entries if possible
	// 100=never use cached entries
	ForceHashPercentage float64

	// Number of files to hash and upload in parallel.
	ParallelUploads int

	// override the directory log level and entry log verbosity.
	OverrideDirLogDetail   *policy.LogDetail
	OverrideEntryLogDetail *policy.LogDetail

	// Fail the entire snapshot on source file/directory error.
	FailFast bool

	// When set to true, do not ignore any files, regardless of policy settings.
	DisableIgnoreRules bool

	// Labels to apply to every checkpoint made for this snapshot.
	CheckpointLabels map[string]string

	nowTimeFunc func() time.Time

	// stats must be allocated on heap to enforce 64-bit alignment due to atomic access on ARM.
	stats *sourceHistogram

	isCanceled atomic.Bool

	// for testing only, when set will write to a given channel whenever checkpoint completes
	checkpointFinished chan struct{}

	// disable snapshot size estimation
	disableEstimation bool

	workerPool *workshare.Pool[*scanWorkItem]

	traceEnabled bool

	summaryMtx sync.Mutex
	summary    sourceHistogram
}

// IsCanceled returns true if the upload is canceled.
func (u *Scanner) IsCanceled() bool {
	return u.incompleteReason() != ""
}

func (u *Scanner) incompleteReason() string {
	if c := u.isCanceled.Load(); c {
		return IncompleteReasonCanceled
	}

	return ""
}

func (s *Scanner) addAllFileStats(size int64) {
	atomic.AddUint64(&s.stats.totalSize, uint64(size))

	switch {
	case size == 0:
		atomic.AddUint32(&s.summary.files.size0Byte, 1)
	case size > 0 && size <= 100*1024: // <= 100KB
		atomic.AddUint32(&s.summary.files.size0bTo100Kb, 1)
	case size > 100*1024 && size <= 100*1024*1024: // > 100KB and <= 100MB
		atomic.AddUint32(&s.summary.files.size100KbTo100Mb, 1)
	case size > 100*1024*1024 && size <= 1024*1024*1024: // > 100MB and <= 1GB
		atomic.AddUint32(&s.summary.files.size100MbTo1Gb, 1)
	case size > 1024*1024*1024: // > 1GB
		atomic.AddUint32(&s.summary.files.sizeOver1Gb, 1)
	}
}

func (s *Scanner) updateFileSummaryInternal(ctx context.Context, f fs.File) {
	atomic.AddUint32(&s.summary.files.totalFiles, 1)
	s.addAllFileStats(f.Size())
}

func (s *Scanner) updateSymlinkStats(ctx context.Context, relativePath string, f fs.Symlink) (ret error) {
	s.Progress.HashingFile(relativePath)

	defer func() {
		s.Progress.FinishedFile(relativePath, ret)
	}()
	defer s.Progress.FinishedHashingFile(relativePath, f.Size())

	atomic.AddUint32(&s.stats.files.totalSymlink, 1)
	s.addAllFileStats(f.Size())

	return nil
}

func (s *Scanner) updateStreamingFileStats(ctx context.Context, relativePath string, f fs.StreamingFile) (ret error) {
	reader, err := f.GetReader(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get streaming file reader")
	}

	defer reader.Close() //nolint:errcheck

	s.Progress.HashingFile(relativePath)
	var streamSize int64

	defer func() {
		s.Progress.FinishedHashingFile(relativePath, streamSize)
		s.Progress.FinishedFile(relativePath, ret)
	}()

	written, err := s.copyWithProgress(io.Discard, reader)
	if err != nil {
		return err
	}

	streamSize = written

	atomic.AddUint32(&s.stats.files.totalFiles, 1)
	s.addAllFileStats(streamSize)

	return nil
}

func (u *Scanner) copyWithProgress(dst io.Writer, src io.Reader) (int64, error) {
	uploadBuf := iocopy.GetBuffer()
	defer iocopy.ReleaseBuffer(uploadBuf)

	var written int64

	for {
		if u.IsCanceled() {
			return 0, errors.Wrap(errCanceled, "canceled when copying data")
		}

		readBytes, readErr := src.Read(uploadBuf)

		if readBytes > 0 {
			wroteBytes, writeErr := dst.Write(uploadBuf[0:readBytes])
			if wroteBytes > 0 {
				written += int64(wroteBytes)
				u.Progress.HashedBytes(int64(wroteBytes))
			}

			if writeErr != nil {
				//nolint:wrapcheck
				return written, writeErr
			}

			if readBytes != wroteBytes {
				return written, io.ErrShortWrite
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}

			//nolint:wrapcheck
			return written, readErr
		}
	}

	return written, nil
}

// scanDirectory scans the specified Directory for stats
func (s *Scanner) scanDirectory(ctx context.Context, rootDir fs.Directory, previousDirs []fs.Directory) error {
	var dmb DirManifestBuilder

	localDirPathOrEmpty := rootDir.LocalFilesystemPath()

	return s.uploadDirInternal(ctx, rootDir, previousDirs, localDirPathOrEmpty, ".", &dmb)
}

func (u *Scanner) processChildren(
	ctx context.Context,
	localDirPathOrEmpty, relativePath string,
	dir fs.Directory,
	previousDirs []fs.Directory,
) error {
	var wg workshare.AsyncGroup[*scanWorkItem]

	// ensure we wait for all work items before returning
	defer wg.Close()

	// ignore errCancel because a more serious error may be reported in wg.Wait()
	// we'll check for cancellation later.

	if err := u.processDirectoryEntries(ctx, localDirPathOrEmpty, relativePath, dir, previousDirs, &wg); err != nil && !errors.Is(err, errCanceled) {
		return err
	}

	for _, wi := range wg.Wait() {
		if wi != nil && wi.err != nil {
			return wi.err
		}
	}

	if u.IsCanceled() {
		return errCanceled
	}

	return nil
}

func (u *Scanner) maybeIgnoreCachedEntry(ctx context.Context, ent fs.Entry) fs.Entry {
	if h, ok := ent.(object.HasObjectID); ok {
		if 100*rand.Float64() < u.ForceHashPercentage { //nolint:gosec
			scannerLog(ctx).Debugw("re-hashing cached object", "oid", h.ObjectID())
			return nil
		}

		return ent
	}

	return nil
}

func (u *Scanner) effectiveParallelFileReads(pol *policy.Policy) int {
	p := u.ParallelUploads
	if p > 0 {
		// command-line override takes precedence.
		return p
	}

	// use policy setting or number of CPUs.
	max := pol.UploadPolicy.MaxParallelFileReads.OrDefault(runtime.NumCPU())
	if p < 1 || p > max {
		return max
	}

	return p
}

func (u *Scanner) processDirectoryEntries(
	ctx context.Context,
	localDirPathOrEmpty string,
	dirRelativePath string,
	dir fs.Directory,
	prevDirs []fs.Directory,
	wg *workshare.AsyncGroup[*scanWorkItem],
) error {
	// processEntryError distinguishes an error thrown when attempting to read a directory.
	type processEntryError struct {
		error
	}

	err := dir.IterateEntries(ctx, func(ctx context.Context, entry fs.Entry) error {
		if u.IsCanceled() {
			return errCanceled
		}

		entryRelativePath := path.Join(dirRelativePath, entry.Name())

		if wg.CanShareWork(u.workerPool) {
			wg.RunAsync(u.workerPool, func(c *workshare.Pool[*scanWorkItem], wi *scanWorkItem) {
				wi.err = u.processSingle(ctx, entry, entryRelativePath, prevDirs, localDirPathOrEmpty)
			}, &scanWorkItem{})
		} else {
			if err := u.processSingle(ctx, entry, entryRelativePath, prevDirs, localDirPathOrEmpty); err != nil {
				return processEntryError{err}
			}
		}

		return nil
	})

	if err == nil {
		return nil
	}

	var peError processEntryError
	if errors.As(err, &peError) {
		return peError.error
	}

	if errors.Is(err, errCanceled) {
		return errCanceled
	}

	return dirReadError{err}
}

func (s *Scanner) findCachedEntry(ctx context.Context, entryRelativePath string, entry fs.Entry, prevDirs []fs.Directory) fs.Entry {
	for _, e := range prevDirs {
		if ent, err := e.Child(ctx, entry.Name()); err == nil {
			switch entry.(type) {
			case fs.StreamingFile:
				if commonMetadataEquals(entry, ent) {
					return ent
				}
			default:
				if metadataEquals(entry, ent) {
					return ent
				}
			}
		}
	}

	return nil
}

//nolint:funlen
func (s *Scanner) processSingle(
	ctx context.Context,
	entry fs.Entry,
	entryRelativePath string,
	prevDirs []fs.Directory,
	localDirPathOrEmpty string,
) error {
	defer entry.Close()

	// note this function runs in parallel and updates 'u.stats', which must be done using atomic operations.
	t0 := timetrack.StartTimer()

	// TODO:
	// if _, ok := entry.(fs.Directory); !ok {
	// 	// See if we had this name during either of previous passes.
	// 	if cachedEntry := s.maybeIgnoreCachedEntry(ctx, findCachedEntry(ctx, entryRelativePath, entry, prevDirs)); cachedEntry != nil {
	// 		atomic.AddUint32(&s.stats.files.totalFiles, 1)
	// 		s.addAllFileStats(cachedEntry.Size())

	// 		s.Progress.CachedFile(entryRelativePath, cachedEntry.Size())

	// 		cachedDirEntry, err := newCachedDirEntry(entry, cachedEntry, entry.Name())

	// 		s.Progress.FinishedFile(entryRelativePath, err)

	// 		if err != nil {
	// 			return errors.Wrap(err, "unable to create dir entry")
	// 		}

	// 		return s.processEntryScanResult(ctx, cachedDirEntry, nil, entryRelativePath,
	// 			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
	// 			"cached", t0)
	// 	}
	// }

	switch entry := entry.(type) {
	case fs.Directory:
		childDirBuilder := &DirManifestBuilder{}

		childLocalDirPathOrEmpty := ""
		if localDirPathOrEmpty != "" {
			childLocalDirPathOrEmpty = filepath.Join(localDirPathOrEmpty, entry.Name())
		}

		childPrevDirs := uniqueChildDirectories(ctx, prevDirs, entry.Name())

		err := s.uploadDirInternal(ctx, entry, childPrevDirs, childLocalDirPathOrEmpty, entryRelativePath, childDirBuilder)
		if errors.Is(err, errCanceled) {
			return err
		}

		if err != nil {
			// Note: This only catches errors in subdirectories of the snapshot root, not on the snapshot
			// root itself. The intention is to always fail if the top level directory can't be read,
			// otherwise a meaningless, empty snapshot is created that can't be restored.
			var dre dirReadError
			if errors.As(err, &dre) {
				s.reportErrorAndMaybeCancel(dre.error, entryRelativePath)
			} else {
				return errors.Wrapf(err, "unable to process directory %q", entry.Name())
			}
		}

		return nil

	case fs.Symlink:
		return s.updateSymlinkStats(ctx, entryRelativePath, entry)

	case fs.File:
		s.updateFileSummaryInternal(ctx, entry)

		return s.processEntryScanResult(ctx, nil, nil, entryRelativePath,
			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
			"snapshotted file", t0)

	case fs.ErrorEntry:
		var prefix string

		if errors.Is(entry.ErrorInfo(), fs.ErrUnknown) {
			prefix = "unknown entry"
		} else {
			prefix = "error"
		}

		return s.processEntryScanResult(ctx, nil, entry.ErrorInfo(), entryRelativePath,
			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
			prefix, t0)

	case fs.StreamingFile:
		err := s.updateStreamingFileStats(ctx, entryRelativePath, entry)

		return s.processEntryScanResult(ctx, nil, err, entryRelativePath,
			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
			"snapshotted streaming file", t0)

	default:
		return errors.Errorf("unexpected entry type: %T %v", entry, entry.Mode())
	}
}

func (u *Scanner) processEntryScanResult(ctx context.Context, de *snapshot.DirEntry, err error, entryRelativePath string, logDetail policy.LogDetail, logMessage string, t0 timetrack.Timer) error {
	if err != nil {
		u.reportErrorAndMaybeCancel(err, entryRelativePath)
	}

	if de != nil {
		maybeLogEntryProcessed(
			scannerLog(ctx),
			logDetail,
			logMessage, entryRelativePath, de, err, t0)
	}

	return nil
}

func (s *Scanner) uploadDirInternal(
	ctx context.Context,
	directory fs.Directory,
	previousDirs []fs.Directory,
	localDirPathOrEmpty, dirRelativePath string,
	thisDirBuilder *DirManifestBuilder,
) (resultErr error) {
	atomic.AddUint32(&s.stats.dirs.totalDirs, 1)

	if s.traceEnabled {
		var span trace.Span

		ctx, span = uploadTracer.Start(ctx, "ScanDir", trace.WithAttributes(attribute.String("dir", dirRelativePath)))
		defer span.End()
	}

	s.Progress.StartedDirectory(dirRelativePath)
	defer s.Progress.FinishedDirectory(dirRelativePath)

	// TOOD: support for shallow dirs

	if err := s.processChildren(ctx, localDirPathOrEmpty, dirRelativePath, directory, uniqueDirectories(previousDirs)); err != nil && !errors.Is(err, errCanceled) {
		return err
	}

	return nil
}

func (u *Scanner) reportErrorAndMaybeCancel(err error, entryRelativePath string) {
	if u.IsCanceled() && errors.Is(err, errCanceled) {
		// already canceled, do not report another.
		return
	}

	atomic.AddUint32(&u.stats.errorCount, 1)

	rc := rootCauseError(err)
	u.Progress.Error(entryRelativePath, rc, false)

	if u.FailFast {
		u.Cancel()
	}
}

// NewScanner creates new Scanner object for a given repository.
func NewScanner() *Scanner {
	return &Scanner{
		Progress: &NullUploadProgress{},
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Scanner) Cancel() {
	u.isCanceled.Store(true)
}

// Scan scans the contents of the specified filesystem entry (file or
// directory) and returns statistics.
func (s *Scanner) Scan(
	ctx context.Context,
	source fs.Entry,
) error {
	ctx, span := uploadTracer.Start(ctx, "Scan")
	defer span.End()

	s.traceEnabled = span.IsRecording()

	s.Progress.UploadStarted()
	defer s.Progress.UploadFinished()

	// set default as 8
	parallel := 8

	scannerLog(ctx).Debugw("scanning", "source", "parallel", parallel)

	s.workerPool = workshare.NewPool[*scanWorkItem](parallel - 1)
	defer s.workerPool.Close()

	s.stats = &sourceHistogram{}

	var err error

	startTime := fs.UTCTimestampFromTime(s.nowTimeFunc())

	switch entry := source.(type) {
	case fs.Directory:
		var previousDirs []fs.Directory
		err = s.scanDirectory(ctx, entry, previousDirs)

	case fs.File:
		s.Progress.EstimatedDataSize(1, entry.Size())
		s.updateFileSummaryInternal(ctx, entry)

	default:
		return errors.Errorf("unsupported source: %v", source)
	}

	if err != nil {
		return rootCauseError(err)
	}

	endTime := fs.UTCTimestampFromTime(s.nowTimeFunc())
	scannerLog(ctx).Infof("Reason: %s, Time Taken: %s, Summary: %#v",
		s.incompleteReason(), startTime.Sub(endTime), s.stats)

	return nil
}
