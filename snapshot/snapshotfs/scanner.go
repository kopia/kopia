package snapshotfs

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/workshare"
	"github.com/kopia/kopia/repo"
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

	// Enable snapshot actions
	EnableActions bool

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

	getTicker func(time.Duration) <-chan time.Time

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

func (u *Scanner) uploadFileInternal(ctx context.Context, parentCheckpointRegistry *checkpointRegistry, relativePath string, f fs.File, pol *policy.Policy) (dirEntry *snapshot.DirEntry, ret error) {
	u.Progress.HashingFile(relativePath)

	defer func() {
		u.Progress.FinishedFile(relativePath, ret)
	}()
	defer u.Progress.FinishedHashingFile(relativePath, f.Size())

	if pf, ok := f.(snapshot.HasDirEntryOrNil); ok {
		switch de, err := pf.DirEntryOrNil(ctx); {
		case err != nil:
			return nil, errors.Wrap(err, "can't read placeholder")
		case err == nil && de != nil:
			// We have read sufficient information from the shallow file's extended
			// attribute to construct DirEntry.
			_, err := u.repo.VerifyObject(ctx, de.ObjectID)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid placeholder for %q contains foreign object.ID", f.Name())
			}

			return de, nil
		}
	}

	comp := pol.CompressionPolicy.CompressorForFile(f)

	chunkSize := pol.UploadPolicy.ParallelUploadAboveSize.OrDefault(-1)
	if chunkSize < 0 || f.Size() <= chunkSize {
		// all data fits in 1 full chunks, upload directly
		return u.uploadFileData(ctx, parentCheckpointRegistry, f, f.Name(), 0, -1, comp)
	}

	// we always have N+1 parts, first N are exactly chunkSize, last one has undetermined length
	fullParts := f.Size() / chunkSize

	// directory entries and errors for partial upload results
	parts := make([]*snapshot.DirEntry, fullParts+1)
	partErrors := make([]error, fullParts+1)

	var wg workshare.AsyncGroup[*uploadWorkItem]
	defer wg.Close()

	for i := 0; i < len(parts); i++ {
		i := i
		offset := int64(i) * chunkSize

		length := chunkSize
		if i == len(parts)-1 {
			// last part has unknown length to accommodate the file that may be growing as we're snapshotting it
			length = -1
		}

		if wg.CanShareWork(u.workerPool) {
			// another goroutine is available, delegate to them
			wg.RunAsync(u.workerPool, func(c *workshare.Pool[*uploadWorkItem], request *uploadWorkItem) {
				parts[i], partErrors[i] = u.uploadFileData(ctx, parentCheckpointRegistry, f, uuid.NewString(), offset, length, comp)
			}, nil)
		} else {
			// just do the work in the current goroutine
			parts[i], partErrors[i] = u.uploadFileData(ctx, parentCheckpointRegistry, f, uuid.NewString(), offset, length, comp)
		}
	}

	wg.Wait()

	// see if we got any errors
	if err := multierr.Combine(partErrors...); err != nil {
		return nil, errors.Wrap(err, "error uploading parts")
	}

	return concatenateParts(ctx, u.repo, f.Name(), parts)
}

func (u *Scanner) uploadSymlinkInternal(ctx context.Context, relativePath string, f fs.Symlink) (dirEntry *snapshot.DirEntry, ret error) {
	u.Progress.HashingFile(relativePath)

	defer func() {
		u.Progress.FinishedFile(relativePath, ret)
	}()
	defer u.Progress.FinishedHashingFile(relativePath, f.Size())

	target, err := f.Readlink(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read symlink")
	}

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "SYMLINK:" + f.Name(),
	})
	defer writer.Close() //nolint:errcheck

	written, err := u.copyWithProgress(writer, bytes.NewBufferString(target))
	if err != nil {
		return nil, err
	}

	r, err := writer.Result()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get result")
	}

	de, err := newDirEntry(f, f.Name(), r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written

	return de, nil
}

func (u *Scanner) uploadStreamingFileInternal(ctx context.Context, relativePath string, f fs.StreamingFile, pol *policy.Policy) (dirEntry *snapshot.DirEntry, ret error) {
	reader, err := f.GetReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get streaming file reader")
	}

	defer reader.Close() //nolint:errcheck

	var streamSize int64

	u.Progress.HashingFile(relativePath)

	defer func() {
		u.Progress.FinishedHashingFile(relativePath, streamSize)
		u.Progress.FinishedFile(relativePath, ret)
	}()

	comp := pol.CompressionPolicy.CompressorForFile(f)
	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "STREAMFILE:" + f.Name(),
		Compressor:  comp,
	})

	defer writer.Close() //nolint:errcheck

	written, err := u.copyWithProgress(writer, reader)
	if err != nil {
		return nil, err
	}

	r, err := writer.Result()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get result")
	}

	de, err := newDirEntry(f, f.Name(), r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written
	streamSize = written

	atomic.AddInt32(&u.stats.TotalFileCount, 1)
	atomic.AddInt64(&u.stats.TotalFileSize, de.FileSize)

	return de, nil
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

// newDirEntryWithSummary makes DirEntry objects for directory Entries that need a DirectorySummary.
func newDirEntryWithSummary(d fs.Entry, oid object.ID, summ *fs.DirectorySummary) (*snapshot.DirEntry, error) {
	de, err := newDirEntry(d, d.Name(), oid)
	if err != nil {
		return nil, err
	}

	de.DirSummary = summ

	return de, nil
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

// uploadFileWithCheckpointing uploads the specified File to the repository.
func (u *Scanner) uploadFileWithCheckpointing(ctx context.Context, relativePath string, file fs.File, pol *policy.Policy, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
	var cp checkpointRegistry

	cancelCheckpointer := u.periodicallyCheckpoint(ctx, &cp, &snapshot.Manifest{Source: sourceInfo})
	defer cancelCheckpointer()

	res, err := u.uploadFileInternal(ctx, &cp, relativePath, file, pol)
	if err != nil {
		return nil, err
	}

	return newDirEntryWithSummary(file, res.ObjectID, &fs.DirectorySummary{
		TotalFileCount: 1,
		TotalFileSize:  res.FileSize,
		MaxModTime:     res.ModTime,
	})
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
func (u *Scanner) uploadDirWithCheckpointing(ctx context.Context, rootDir fs.Directory, policyTree *policy.Tree, previousDirs []fs.Directory, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
	var (
		dmb DirManifestBuilder
		cp  checkpointRegistry
	)

	var hc actionContext

	localDirPathOrEmpty := rootDir.LocalFilesystemPath()

	overrideDir, err := u.executeBeforeFolderAction(ctx, "before-snapshot-root", policyTree.EffectivePolicy().Actions.BeforeSnapshotRoot, localDirPathOrEmpty, &hc)
	if err != nil {
		return nil, dirReadError{errors.Wrap(err, "error executing before-snapshot-root action")}
	}

	if overrideDir != nil {
		rootDir = u.wrapIgnorefs(scannerLog(ctx), overrideDir, policyTree, true)
	}

	defer u.executeAfterFolderAction(ctx, "after-snapshot-root", policyTree.EffectivePolicy().Actions.AfterSnapshotRoot, localDirPathOrEmpty, &hc)

	return uploadDirInternal(ctx, u, rootDir, policyTree, previousDirs, localDirPathOrEmpty, ".", &dmb, &cp)
}

type uploadWorkItem struct {
	err error
}

func isDir(e *snapshot.DirEntry) bool {
	return e.Type == snapshot.EntryTypeDirectory
}

func (u *Scanner) processChildren(
	ctx context.Context,
	parentDirCheckpointRegistry *checkpointRegistry,
	parentDirBuilder *DirManifestBuilder,
	localDirPathOrEmpty, relativePath string,
	dir fs.Directory,
	policyTree *policy.Tree,
	previousDirs []fs.Directory,
) error {
	var wg workshare.AsyncGroup[*uploadWorkItem]

	// ensure we wait for all work items before returning
	defer wg.Close()

	// ignore errCancel because a more serious error may be reported in wg.Wait()
	// we'll check for cancellation later.

	if err := u.processDirectoryEntries(ctx, parentDirCheckpointRegistry, parentDirBuilder, localDirPathOrEmpty, relativePath, dir, policyTree, previousDirs, &wg); err != nil && !errors.Is(err, errCanceled) {
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

func commonMetadataEquals(e1, e2 fs.Entry) bool {
	if l, r := e1.ModTime(), e2.ModTime(); !l.Equal(r) {
		return false
	}

	if l, r := e1.Mode(), e2.Mode(); l != r {
		return false
	}

	if l, r := e1.Owner(), e2.Owner(); l != r {
		return false
	}

	return true
}

func metadataEquals(e1, e2 fs.Entry) bool {
	if !commonMetadataEquals(e1, e2) {
		return false
	}

	if l, r := e1.Size(), e2.Size(); l != r {
		return false
	}

	return true
}

func findCachedEntry(ctx context.Context, entryRelativePath string, entry fs.Entry, prevDirs []fs.Directory, pol *policy.Tree) fs.Entry {
	var missedEntry fs.Entry

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

			missedEntry = ent
		}
	}

	if missedEntry != nil {
		if pol.EffectivePolicy().LoggingPolicy.Entries.CacheMiss.OrDefault(policy.LogDetailNone) >= policy.LogDetailNormal {
			scannerLog(ctx).Debugw(
				"cache miss",
				"path", entryRelativePath,
				"mode", missedEntry.Mode().String(),
				"size", missedEntry.Size(),
				"mtime", missedEntry.ModTime())
		}
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
	parentCheckpointRegistry *checkpointRegistry,
	parentDirBuilder *DirManifestBuilder,
	localDirPathOrEmpty string,
	dirRelativePath string,
	dir fs.Directory,
	policyTree *policy.Tree,
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
				wi.err = u.processSingle(ctx, entry, entryRelativePath, parentDirBuilder, policyTree, prevDirs, localDirPathOrEmpty, parentCheckpointRegistry)
			}, &uploadWorkItem{})
		} else {
			if err := u.processSingle(ctx, entry, entryRelativePath, parentDirBuilder, policyTree, prevDirs, localDirPathOrEmpty, parentCheckpointRegistry); err != nil {
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

//nolint:funlen
func (u *Scanner) processSingle(
	ctx context.Context,
	entry fs.Entry,
	entryRelativePath string,
	parentDirBuilder *DirManifestBuilder,
	policyTree *policy.Tree,
	prevDirs []fs.Directory,
	localDirPathOrEmpty string,
	parentCheckpointRegistry *checkpointRegistry,
) error {
	defer entry.Close()

	// note this function runs in parallel and updates 'u.stats', which must be done using atomic operations.
	t0 := timetrack.StartTimer()

	if _, ok := entry.(fs.Directory); !ok {
		// See if we had this name during either of previous passes.
		if cachedEntry := u.maybeIgnoreCachedEntry(ctx, findCachedEntry(ctx, entryRelativePath, entry, prevDirs, policyTree)); cachedEntry != nil {
			atomic.AddInt32(&u.stats.CachedFiles, 1)
			atomic.AddInt64(&u.stats.TotalFileSize, cachedEntry.Size())
			u.Progress.CachedFile(entryRelativePath, cachedEntry.Size())

			cachedDirEntry, err := newCachedDirEntry(entry, cachedEntry, entry.Name())

			u.Progress.FinishedFile(entryRelativePath, err)

			if err != nil {
				return errors.Wrap(err, "unable to create dir entry")
			}

			return u.processEntryUploadResult(ctx, cachedDirEntry, nil, entryRelativePath, parentDirBuilder,
				false,
				u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.CacheHit.OrDefault(policy.LogDetailNone)),
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

		childTree := policyTree.Child(entry.Name())
		childPrevDirs := uniqueChildDirectories(ctx, prevDirs, entry.Name())

		de, err := uploadDirInternal(ctx, u, entry, childTree, childPrevDirs, childLocalDirPathOrEmpty, entryRelativePath, childDirBuilder, parentCheckpointRegistry)
		if errors.Is(err, errCanceled) {
			return err
		}

		if err != nil {
			// Note: This only catches errors in subdirectories of the snapshot root, not on the snapshot
			// root itself. The intention is to always fail if the top level directory can't be read,
			// otherwise a meaningless, empty snapshot is created that can't be restored.
			var dre dirReadError
			if errors.As(err, &dre) {
				isIgnoredError := childTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreDirectoryErrors.OrDefault(false)
				u.reportErrorAndMaybeCancel(dre.error, isIgnoredError, parentDirBuilder, entryRelativePath)
			} else {
				return errors.Wrapf(err, "unable to process directory %q", entry.Name())
			}
		} else {
			parentDirBuilder.AddEntry(de)
		}

		return nil

	case fs.Symlink:
		de, err := u.uploadSymlinkInternal(ctx, entryRelativePath, entry)

		return u.processEntryUploadResult(ctx, de, err, entryRelativePath, parentDirBuilder,
			policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false),
			u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			"snapshotted symlink", t0)

	case fs.File:
		atomic.AddInt32(&u.stats.NonCachedFiles, 1)

		// de, err := u.uploadFileInternal(ctx, parentCheckpointRegistry, entryRelativePath, entry, policyTree.Child(entry.Name()).EffectivePolicy())
		de, err := u.updateFileSummaryInternal(ctx, entry)

		return u.processEntryUploadResult(ctx, de, err, entryRelativePath, parentDirBuilder,
			policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false),
			u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			"snapshotted file", t0)

	case fs.ErrorEntry:
		var (
			isIgnoredError bool
			prefix         string
		)

		if errors.Is(entry.ErrorInfo(), fs.ErrUnknown) {
			isIgnoredError = policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreUnknownTypes.OrDefault(true)
			prefix = "unknown entry"
		} else {
			isIgnoredError = policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false)
			prefix = "error"
		}

		return u.processEntryUploadResult(ctx, nil, entry.ErrorInfo(), entryRelativePath, parentDirBuilder,
			isIgnoredError,
			u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			prefix, t0)

	case fs.StreamingFile:
		atomic.AddInt32(&u.stats.NonCachedFiles, 1)

		de, err := u.uploadStreamingFileInternal(ctx, entryRelativePath, entry, policyTree.Child(entry.Name()).EffectivePolicy())

		return u.processEntryUploadResult(ctx, de, err, entryRelativePath, parentDirBuilder,
			policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false),
			u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			"snapshotted streaming file", t0)

	default:
		return errors.Errorf("unexpected entry type: %T %v", entry, entry.Mode())
	}
}

func (u *Scanner) processEntryUploadResult(ctx context.Context, de *snapshot.DirEntry, err error, entryRelativePath string, parentDirBuilder *DirManifestBuilder, isIgnored bool, logDetail policy.LogDetail, logMessage string, t0 timetrack.Timer) error {
	if err != nil {
		u.reportErrorAndMaybeCancel(err, isIgnored, parentDirBuilder, entryRelativePath)
	} else {
		parentDirBuilder.AddEntry(de)
	}

	maybeLogEntryProcessed(
		scannerLog(ctx),
		logDetail,
		logMessage, entryRelativePath, de, err, t0)

	return nil
}

func uploadDirInternal(
	ctx context.Context,
	u *Scanner,
	directory fs.Directory,
	policyTree *policy.Tree,
	previousDirs []fs.Directory,
	localDirPathOrEmpty, dirRelativePath string,
	thisDirBuilder *DirManifestBuilder,
	thisCheckpointRegistry *checkpointRegistry,
) (resultDE *snapshot.DirEntry, resultErr error) {
	atomic.AddInt32(&u.stats.TotalDirectoryCount, 1)

	if u.traceEnabled {
		var span trace.Span

		ctx, span = uploadTracer.Start(ctx, "UploadDir", trace.WithAttributes(attribute.String("dir", dirRelativePath)))
		defer span.End()
	}

	t0 := timetrack.StartTimer()

	defer func() {
		maybeLogEntryProcessed(
			scannerLog(ctx),
			u.OverrideDirLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Directories.Snapshotted.OrDefault(policy.LogDetailNone)),
			"snapshotted directory", dirRelativePath, resultDE, resultErr, t0)
	}()

	u.Progress.StartedDirectory(dirRelativePath)
	defer u.Progress.FinishedDirectory(dirRelativePath)

	var definedActions policy.ActionsPolicy

	if p := policyTree.DefinedPolicy(); p != nil {
		definedActions = p.Actions
	}

	var hc actionContext
	defer cleanupActionContext(ctx, &hc)

	overrideDir, herr := u.executeBeforeFolderAction(ctx, "before-folder", definedActions.BeforeFolder, localDirPathOrEmpty, &hc)
	if herr != nil {
		return nil, dirReadError{errors.Wrap(herr, "error executing before-folder action")}
	}

	defer u.executeAfterFolderAction(ctx, "after-folder", definedActions.AfterFolder, localDirPathOrEmpty, &hc)

	if overrideDir != nil {
		directory = u.wrapIgnorefs(scannerLog(ctx), overrideDir, policyTree, true)
	}

	if de, err := uploadShallowDirInternal(ctx, directory, u); de != nil || err != nil {
		return de, err
	}

	childCheckpointRegistry := &checkpointRegistry{}

	thisCheckpointRegistry.addCheckpointCallback(directory.Name(), func() (*snapshot.DirEntry, error) {
		// when snapshotting the parent, snapshot all our children and tell them to populate
		// childCheckpointBuilder
		thisCheckpointBuilder := thisDirBuilder.Clone()

		// invoke all child checkpoints which will populate thisCheckpointBuilder.
		if err := childCheckpointRegistry.runCheckpoints(thisCheckpointBuilder); err != nil {
			return nil, errors.Wrapf(err, "error checkpointing children")
		}

		checkpointManifest := thisCheckpointBuilder.Build(fs.UTCTimestampFromTime(directory.ModTime()), IncompleteReasonCheckpoint)
		oid, err := writeDirManifest(ctx, u.repo, dirRelativePath, checkpointManifest)
		if err != nil {
			return nil, errors.Wrap(err, "error writing dir manifest")
		}

		return newDirEntryWithSummary(directory, oid, checkpointManifest.Summary)
	})
	defer thisCheckpointRegistry.removeCheckpointCallback(directory.Name())

	if err := u.processChildren(ctx, childCheckpointRegistry, thisDirBuilder, localDirPathOrEmpty, dirRelativePath, directory, policyTree, uniqueDirectories(previousDirs)); err != nil && !errors.Is(err, errCanceled) {
		return nil, err
	}

	dirManifest := thisDirBuilder.Build(fs.UTCTimestampFromTime(directory.ModTime()), u.incompleteReason())

	oid, err := writeDirManifest(ctx, u.repo, dirRelativePath, dirManifest)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing dir manifest: %v", directory.Name())
	}

	return newDirEntryWithSummary(directory, oid, dirManifest.Summary)
}

func (u *Scanner) reportErrorAndMaybeCancel(err error, isIgnored bool, dmb *DirManifestBuilder, entryRelativePath string) {
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
	dmb.AddFailedEntry(entryRelativePath, isIgnored, rc)

	if u.FailFast && !isIgnored {
		u.Cancel()
	}
}

// NewUploader creates new Scanner object for a given repository.
func NewScanner(r repo.RepositoryWriter) *Scanner {
	return &Scanner{
		repo:          r,
		Progress:      &NullUploadProgress{},
		EnableActions: r.ClientOptions().EnableActions,
		getTicker:     time.Tick,
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Scanner) Cancel() {
	u.isCanceled.Store(true)
}

func (u *Scanner) maybeOpenDirectoryFromManifest(ctx context.Context, man *snapshot.Manifest) fs.Directory {
	if man == nil {
		return nil
	}

	ent := EntryFromDirEntry(u.repo, man.RootEntry)

	dir, ok := ent.(fs.Directory)
	if !ok {
		scannerLog(ctx).Debugf("previous manifest root is not a directory (was %T %+v)", ent, man.RootEntry)
		return nil
	}

	return dir
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and returns snapshot.Manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up uploads by utilizing hash cache.
func (s *Scanner) Upload(
	ctx context.Context,
	source fs.Entry,
	policyTree *policy.Tree,
	sourceInfo snapshot.SourceInfo,
	previousManifests ...*snapshot.Manifest,
) error {
	ctx, span := uploadTracer.Start(ctx, "Scan")
	defer span.End()

	s.traceEnabled = span.IsRecording()

	s.Progress.UploadStarted()
	defer s.Progress.UploadFinished()

	parallel := s.effectiveParallelFileReads(policyTree.EffectivePolicy())

	scannerLog(ctx).Debugw("uploading", "source", sourceInfo, "previousManifests", len(previousManifests), "parallel", parallel)

	s.workerPool = workshare.NewPool[*uploadWorkItem](parallel - 1)
	defer s.workerPool.Close()

	s.stats = &sourceHistogram{}

	var err error

	startTime := fs.UTCTimestampFromTime(s.nowTimeFunc())

	switch entry := source.(type) {
	case fs.Directory:
		var previousDirs []fs.Directory

		for _, m := range previousManifests {
			if d := s.maybeOpenDirectoryFromManifest(ctx, m); d != nil {
				previousDirs = append(previousDirs, d)
			}
		}

		wrapped := s.wrapIgnorefs(scannerLog(ctx), entry, policyTree, true /* reportIgnoreStats */)

		err = s.uploadDirWithCheckpointing(ctx, wrapped, policyTree, previousDirs, sourceInfo)

	case fs.File:
		s.Progress.EstimatedDataSize(1, entry.Size())
		err = s.uploadFileWithCheckpointing(ctx, entry.Name(), entry, policyTree.EffectivePolicy(), sourceInfo)

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

func (u *Scanner) wrapIgnorefs(logger logging.Logger, entry fs.Directory, policyTree *policy.Tree, reportIgnoreStats bool) fs.Directory {
	if u.DisableIgnoreRules {
		return entry
	}

	return ignorefs.New(entry, policyTree, ignorefs.ReportIgnoredFiles(func(ctx context.Context, fname string, md fs.Entry, policyTree *policy.Tree) {
		if md.IsDir() {
			maybeLogEntryProcessed(
				logger,
				policyTree.EffectivePolicy().LoggingPolicy.Directories.Ignored.OrDefault(policy.LogDetailNone),
				"ignored directory", fname, nil, nil, timetrack.StartTimer())

			if reportIgnoreStats {
				u.Progress.ExcludedDir(fname)
			}
		} else {
			maybeLogEntryProcessed(
				logger,
				policyTree.EffectivePolicy().LoggingPolicy.Entries.Ignored.OrDefault(policy.LogDetailNone),
				"ignored", fname, nil, nil, timetrack.StartTimer())

			if reportIgnoreStats {
				u.Progress.ExcludedFile(fname, md.Size())
			}
		}

		u.stats.AddExcluded(md)
	}))
}
