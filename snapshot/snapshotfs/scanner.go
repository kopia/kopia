package snapshotfs

import (
	"context"
	"io"
	"io/ioutil"
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
	totalSize uint64
	files     fileHistogram
	dirs      dirHistogram
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

	workerPool *workshare.Pool[*uploadWorkItem]

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

func (u *Scanner) updateFileSummaryInternal(ctx context.Context, f fs.File) {
	var wg workshare.AsyncGroup[*uploadWorkItem]
	defer wg.Close()

	updater := func() {
		atomic.AddUint32(&u.summary.files.totalFiles, 1)

		size := f.Size()
		switch {
		case size == 0:
			atomic.AddUint32(&u.summary.files.size0Byte, 1)
		case size > 0 && size <= 100*1024: // <= 100KB
			atomic.AddUint32(&u.summary.files.size0bTo100Kb, 1)
		case size > 100*1024 && size <= 100*1024*1024: // > 100KB and <= 100MB
			atomic.AddUint32(&u.summary.files.size100KbTo100Mb, 1)
		case size > 100*1024*1024 && size <= 1024*1024*1024: // > 100MB and <= 1GB
			atomic.AddUint32(&u.summary.files.size100MbTo1Gb, 1)
		case size > 1024*1024*1024: // > 1GB
			atomic.AddUint32(&u.summary.files.sizeOver1Gb, 1)
		}
	}

	// this is a shared workpool
	if wg.CanShareWork(u.workerPool) {
		// another goroutine is available, delegate to them
		wg.RunAsync(u.workerPool, func(c *workshare.Pool[*uploadWorkItem], request *uploadWorkItem) {
			updater()
		}, nil)
	} else {
		updater()
	}

	wg.Wait()
}

func (s *Scanner) updateSymlinkStats(ctx context.Context, relativePath string, f fs.Symlink) (ret error) {
	s.Progress.HashingFile(relativePath)

	defer func() {
		s.Progress.FinishedFile(relativePath, ret)
	}()
	defer s.Progress.FinishedHashingFile(relativePath, f.Size())

	atomic.AddUint32(&s.stats.files.totalSymlink, 1)
	atomic.AddUint64(&s.stats.totalSize, uint64(f.Size()))

	return nil
}

func (u *Scanner) updateStreamingFileStats(ctx context.Context, relativePath string, f fs.StreamingFile) (ret error) {
	reader, err := f.GetReader(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get streaming file reader")
	}

	defer reader.Close() //nolint:errcheck

	u.Progress.HashingFile(relativePath)
	var streamSize int64

	defer func() {
		u.Progress.FinishedHashingFile(relativePath, streamSize)
		u.Progress.FinishedFile(relativePath, ret)
	}()

	written, err := u.copyWithProgress(ioutil.Discard, reader)
	if err != nil {
		return err
	}

	streamSize = written

	atomic.AddUint32(&u.stats.files.totalFiles, 1)
	atomic.AddUint64(&u.stats.totalSize, uint64(streamSize))

	switch {
	case streamSize == 0:
		atomic.AddUint32(&u.summary.files.size0Byte, 1)
	case streamSize > 0 && streamSize <= 100*1024: // <= 100KB
		atomic.AddUint32(&u.summary.files.size0bTo100Kb, 1)
	case streamSize > 100*1024 && streamSize <= 100*1024*1024: // > 100KB and <= 100MB
		atomic.AddUint32(&u.summary.files.size100KbTo100Mb, 1)
	case streamSize > 100*1024*1024 && streamSize <= 1024*1024*1024: // > 100MB and <= 1GB
		atomic.AddUint32(&u.summary.files.size100MbTo1Gb, 1)
	case streamSize > 1024*1024*1024: // > 1GB
		atomic.AddUint32(&u.summary.files.sizeOver1Gb, 1)
	}

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

// newDirEntry makes DirEntry objects for any type of Entry.
func newDirEntry(md fs.Entry, fname string, oid object.ID) (*snapshot.DirEntry, error) {
	var entryType snapshot.EntryType

	switch md := md.(type) {
	case fs.Directory:
		entryType = snapshot.EntryTypeDirectory
	case fs.Symlink:
		entryType = snapshot.EntryTypeSymlink
	case fs.File, fs.StreamingFile:
		entryType = snapshot.EntryTypeFile
	default:
		return nil, errors.Errorf("invalid entry type %T", md)
	}

	return &snapshot.DirEntry{
		Name:        fname,
		Type:        entryType,
		Permissions: snapshot.Permissions(md.Mode() & fs.ModBits),
		FileSize:    md.Size(),
		ModTime:     fs.UTCTimestampFromTime(md.ModTime()),
		UserID:      md.Owner().UserID,
		GroupID:     md.Owner().GroupID,
		ObjectID:    oid,
	}, nil
}

// newCachedDirEntry makes DirEntry objects for entries that are also in
// previous snapshots. It ensures file sizes are populated correctly for
// StreamingFiles.
func newCachedDirEntry(md, cached fs.Entry, fname string) (*snapshot.DirEntry, error) {
	hoid, ok := cached.(object.HasObjectID)
	if !ok {
		return nil, errors.New("cached entry does not implement HasObjectID")
	}

	if _, ok := md.(fs.StreamingFile); ok {
		return newDirEntry(cached, fname, hoid.ObjectID())
	}

	return newDirEntry(md, fname, hoid.ObjectID())
}

// checkpointRoot invokes checkpoints on the provided registry and if a checkpoint entry was generated,
// saves it in an incomplete snapshot manifest.
func (u *Scanner) checkpointRoot(ctx context.Context, cp *checkpointRegistry, prototypeManifest *snapshot.Manifest) error {
	var dmbCheckpoint DirManifestBuilder
	if err := cp.runCheckpoints(&dmbCheckpoint); err != nil {
		return errors.Wrap(err, "running checkpointers")
	}

	checkpointManifest := dmbCheckpoint.Build(fs.UTCTimestampFromTime(u.repo.Time()), "dummy")
	if len(checkpointManifest.Entries) == 0 {
		// did not produce a checkpoint, that's ok
		return nil
	}

	if len(checkpointManifest.Entries) > 1 {
		return errors.Errorf("produced more than one checkpoint: %v", len(checkpointManifest.Entries))
	}

	rootEntry := checkpointManifest.Entries[0]

	scannerLog(ctx).Debugf("checkpointed root %v", rootEntry.ObjectID)

	man := *prototypeManifest
	man.RootEntry = rootEntry
	man.EndTime = fs.UTCTimestampFromTime(u.repo.Time())
	man.StartTime = man.EndTime
	man.IncompleteReason = IncompleteReasonCheckpoint
	man.Tags = u.CheckpointLabels

	if _, err := snapshot.SaveSnapshot(ctx, u.repo, &man); err != nil {
		return errors.Wrap(err, "error saving checkpoint snapshot")
	}

	if _, err := policy.ApplyRetentionPolicy(ctx, u.repo, man.Source, true); err != nil {
		return errors.Wrap(err, "unable to apply retention policy")
	}

	if err := u.repo.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing after checkpoint")
	}

	return nil
}

// uploadDirWithCheckpointing uploads the specified Directory to the repository.
func (s *Scanner) uploadDirWithCheckpointing(ctx context.Context, rootDir fs.Directory, previousDirs []fs.Directory, sourceInfo snapshot.SourceInfo) error {
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
	var wg workshare.AsyncGroup[*uploadWorkItem]

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
	wg *workshare.AsyncGroup[*uploadWorkItem],
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
			wg.RunAsync(u.workerPool, func(c *workshare.Pool[*uploadWorkItem], wi *uploadWorkItem) {
				wi.err = u.processSingle(ctx, entry, entryRelativePath, prevDirs, localDirPathOrEmpty)
			}, &uploadWorkItem{})
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

	if _, ok := entry.(fs.Directory); !ok {
		// See if we had this name during either of previous passes.
		if cachedEntry := s.maybeIgnoreCachedEntry(ctx, findCachedEntry(ctx, entryRelativePath, entry, prevDirs)); cachedEntry != nil {
			atomic.AddInt32(&s.stats.CachedFiles, 1)
			atomic.AddInt64(&s.stats.TotalFileSize, cachedEntry.Size())
			s.Progress.CachedFile(entryRelativePath, cachedEntry.Size())

			cachedDirEntry, err := newCachedDirEntry(entry, cachedEntry, entry.Name())

			s.Progress.FinishedFile(entryRelativePath, err)

			if err != nil {
				return errors.Wrap(err, "unable to create dir entry")
			}

			return s.processEntryScanResult(ctx, cachedDirEntry, nil, entryRelativePath,
				false,
				s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
				"cached", t0)
		}
	}

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
				s.reportErrorAndMaybeCancel(dre.error, false, entryRelativePath)
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
			false,
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
			false,
			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
			prefix, t0)

	case fs.StreamingFile:
		err := s.updateStreamingFileStats(ctx, entryRelativePath, entry)

		return s.processEntryScanResult(ctx, nil, err, entryRelativePath,
			false,
			s.OverrideEntryLogDetail.OrDefault(policy.LogDetailNormal),
			"snapshotted streaming file", t0)

	default:
		return errors.Errorf("unexpected entry type: %T %v", entry, entry.Mode())
	}
}

func (u *Scanner) processEntryScanResult(ctx context.Context, de *snapshot.DirEntry, err error, entryRelativePath string, isIgnored bool, logDetail policy.LogDetail, logMessage string, t0 timetrack.Timer) error {
	if err != nil {
		u.reportErrorAndMaybeCancel(err, isIgnored, entryRelativePath)
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
	atomic.AddInt32(&u.stats.TotalDirectoryCount, 1)

	if s.traceEnabled {
		var span trace.Span

		ctx, span = uploadTracer.Start(ctx, "ScanDir", trace.WithAttributes(attribute.String("dir", dirRelativePath)))
		defer span.End()
	}

	s.Progress.StartedDirectory(dirRelativePath)
	defer s.Progress.FinishedDirectory(dirRelativePath)

	// TOOD: support for shallow dirs

	if err := s.processChildren(ctx, thisDirBuilder, localDirPathOrEmpty, dirRelativePath, directory, uniqueDirectories(previousDirs)); err != nil && !errors.Is(err, errCanceled) {
		return err
	}

	return nil
}

func (u *Scanner) reportErrorAndMaybeCancel(err error, isIgnored bool, entryRelativePath string) {
	if u.IsCanceled() && errors.Is(err, errCanceled) {
		// already canceled, do not report another.
		return
	}

	if isIgnored {
		atomic.AddInt32(&u.stats.IgnoredErrorCount, 1)
	} else {
		atomic.AddInt32(&u.stats.ErrorCount, 1)
	}

	rc := rootCauseError(err)
	u.Progress.Error(entryRelativePath, rc, isIgnored)

	if u.FailFast && !isIgnored {
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

// Scan uploads contents of the specified filesystem entry (file or directory) to the repository and returns snapshot.Manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up uploads by utilizing hash cache.
func (s *Scanner) Scan(
	ctx context.Context,
	source fs.Entry,
	sourceInfo snapshot.SourceInfo,
) error {
	ctx, span := uploadTracer.Start(ctx, "Scan")
	defer span.End()

	s.traceEnabled = span.IsRecording()

	s.Progress.UploadStarted()
	defer s.Progress.UploadFinished()

	// set default as 8
	parallel := 8

	scannerLog(ctx).Debugw("uploading", "source", sourceInfo, "parallel", parallel)

	s.workerPool = workshare.NewPool[*uploadWorkItem](parallel - 1)
	defer s.workerPool.Close()

	s.stats = &sourceHistogram{}

	var err error

	startTime := fs.UTCTimestampFromTime(s.nowTimeFunc())

	switch entry := source.(type) {
	case fs.Directory:
		var previousDirs []fs.Directory
		err = s.uploadDirWithCheckpointing(ctx, entry, previousDirs, sourceInfo)

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
