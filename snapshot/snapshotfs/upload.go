package snapshotfs

import (
	"bytes"
	"context"
	stderrors "errors"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/workshare"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// DefaultCheckpointInterval is the default frequency of mid-upload checkpointing.
const DefaultCheckpointInterval = 45 * time.Minute

var (
	uploadLog = logging.Module("uploader")
	repoFSLog = logging.Module("repofs")

	uploadTracer = otel.Tracer("upload")
)

// minimal detail levels to emit particular pieces of log information.
const (
	minDetailLevelDuration = 1
	minDetailLevelSize     = 3
	minDetailLevelDirStats = 5
	minDetailLevelModTime  = 6
	minDetailLevelOID      = 7
)

var errCanceled = errors.New("canceled")

// reasons why a snapshot is incomplete.
const (
	IncompleteReasonCheckpoint   = "checkpoint"
	IncompleteReasonCanceled     = "canceled"
	IncompleteReasonLimitReached = "limit reached"
)

// Uploader supports efficient uploading files and directories to repository.
type Uploader struct {
	totalWrittenBytes atomic.Int64

	Progress UploadProgress

	// automatically cancel the Upload after certain number of bytes
	MaxUploadBytes int64

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

	// How frequently to create checkpoint snapshot entries.
	CheckpointInterval time.Duration

	// When set to true, do not ignore any files, regardless of policy settings.
	DisableIgnoreRules bool

	// Labels to apply to every checkpoint made for this snapshot.
	CheckpointLabels map[string]string

	repo repo.RepositoryWriter

	// stats must be allocated on heap to enforce 64-bit alignment due to atomic access on ARM.
	stats *snapshot.Stats

	isCanceled atomic.Bool

	getTicker func(time.Duration) <-chan time.Time

	// for testing only, when set will write to a given channel whenever checkpoint completes
	checkpointFinished chan struct{}

	// disable snapshot size estimation
	disableEstimation bool

	workerPool *workshare.Pool[*uploadWorkItem]

	traceEnabled bool
}

// IsCanceled returns true if the upload is canceled.
func (u *Uploader) IsCanceled() bool {
	return u.incompleteReason() != ""
}

func (u *Uploader) incompleteReason() string {
	if c := u.isCanceled.Load(); c {
		return IncompleteReasonCanceled
	}

	wb := u.totalWrittenBytes.Load()
	if mub := u.MaxUploadBytes; mub > 0 && wb > mub {
		return IncompleteReasonLimitReached
	}

	return ""
}

func (u *Uploader) uploadFileInternal(ctx context.Context, parentCheckpointRegistry *checkpointRegistry, relativePath string, f fs.File, pol *policy.Policy) (dirEntry *snapshot.DirEntry, ret error) {
	u.Progress.HashingFile(relativePath)

	defer func() {
		u.Progress.FinishedFile(relativePath, ret)
	}()
	defer u.Progress.FinishedHashingFile(relativePath, f.Size())

	if pf, ok := f.(snapshot.HasDirEntryOrNil); ok {
		if de, err := pf.DirEntryOrNil(ctx); err != nil {
			return nil, errors.Wrap(err, "can't read placeholder")
		} else if de != nil {
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
	metadataComp := pol.MetadataCompressionPolicy.MetadataCompressor()
	splitterName := pol.SplitterPolicy.SplitterForFile(f)

	chunkSize := pol.UploadPolicy.ParallelUploadAboveSize.OrDefault(-1)
	if chunkSize < 0 || f.Size() <= chunkSize {
		// all data fits in 1 full chunks, upload directly
		return u.uploadFileData(ctx, parentCheckpointRegistry, f, f.Name(), 0, -1, comp, metadataComp, splitterName)
	}

	// we always have N+1 parts, first N are exactly chunkSize, last one has undetermined length
	fullParts := f.Size() / chunkSize

	// directory entries and errors for partial upload results
	parts := make([]*snapshot.DirEntry, fullParts+1)
	partErrors := make([]error, fullParts+1)

	var wg workshare.AsyncGroup[*uploadWorkItem]
	defer wg.Close()

	for i := range parts {
		offset := int64(i) * chunkSize

		length := chunkSize
		if i == len(parts)-1 {
			// last part has unknown length to accommodate the file that may be growing as we're snapshotting it
			length = -1
		}

		if wg.CanShareWork(u.workerPool) {
			// another goroutine is available, delegate to them
			wg.RunAsync(u.workerPool, func(_ *workshare.Pool[*uploadWorkItem], _ *uploadWorkItem) {
				parts[i], partErrors[i] = u.uploadFileData(ctx, parentCheckpointRegistry, f, uuid.NewString(), offset, length, comp, metadataComp, splitterName)
			}, nil)
		} else {
			// just do the work in the current goroutine
			parts[i], partErrors[i] = u.uploadFileData(ctx, parentCheckpointRegistry, f, uuid.NewString(), offset, length, comp, metadataComp, splitterName)
		}
	}

	wg.Wait()

	// see if we got any errors
	if err := stderrors.Join(partErrors...); err != nil {
		return nil, errors.Wrap(err, "error uploading parts")
	}

	return concatenateParts(ctx, u.repo, f.Name(), parts, metadataComp)
}

func concatenateParts(ctx context.Context, rep repo.RepositoryWriter, name string, parts []*snapshot.DirEntry, metadataComp compression.Name) (*snapshot.DirEntry, error) {
	var (
		objectIDs []object.ID
		totalSize int64
	)

	// resulting size is the sum of all parts and resulting object ID is concatenation of individual object IDs.
	for _, part := range parts {
		totalSize += part.FileSize
		objectIDs = append(objectIDs, part.ObjectID)
	}

	resultObject, err := rep.ConcatenateObjects(ctx, objectIDs, repo.ConcatenateOptions{Compressor: metadataComp})
	if err != nil {
		return nil, errors.Wrap(err, "concatenate")
	}

	de := parts[0]
	de.Name = name
	de.FileSize = totalSize
	de.ObjectID = resultObject

	return de, nil
}

func (u *Uploader) uploadFileData(ctx context.Context, parentCheckpointRegistry *checkpointRegistry, f fs.File, fname string, offset, length int64, compressor, metadataComp compression.Name, splitterName string) (*snapshot.DirEntry, error) {
	file, err := f.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open file")
	}
	defer file.Close() //nolint:errcheck

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description:        "FILE:" + fname,
		Compressor:         compressor,
		MetadataCompressor: metadataComp,
		Splitter:           splitterName,
		AsyncWrites:        1, // upload chunk in parallel to writing another chunk
	})
	defer writer.Close() //nolint:errcheck

	parentCheckpointRegistry.addCheckpointCallback(fname, func() (*snapshot.DirEntry, error) {
		checkpointID, err := writer.Checkpoint()
		if err != nil {
			return nil, errors.Wrap(err, "checkpoint error")
		}

		if checkpointID == object.EmptyID {
			return nil, nil
		}

		return newDirEntry(f, fname, checkpointID)
	})

	defer parentCheckpointRegistry.removeCheckpointCallback(fname)

	if offset != 0 {
		if _, serr := file.Seek(offset, io.SeekStart); serr != nil {
			return nil, errors.Wrap(serr, "seek error")
		}
	}

	var s io.Reader = file
	if length >= 0 {
		s = io.LimitReader(s, length)
	}

	written, err := u.copyWithProgress(writer, s)
	if err != nil {
		return nil, err
	}

	r, err := writer.Result()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get result")
	}

	de, err := newDirEntry(f, fname, r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written

	atomic.AddInt32(&u.stats.TotalFileCount, 1)
	atomic.AddInt64(&u.stats.TotalFileSize, de.FileSize)

	return de, nil
}

func (u *Uploader) uploadSymlinkInternal(ctx context.Context, relativePath string, f fs.Symlink, metadataComp compression.Name) (dirEntry *snapshot.DirEntry, ret error) {
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
		Description:        "SYMLINK:" + f.Name(),
		MetadataCompressor: metadataComp,
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

func (u *Uploader) uploadStreamingFileInternal(ctx context.Context, relativePath string, f fs.StreamingFile, pol *policy.Policy) (dirEntry *snapshot.DirEntry, ret error) {
	reader, err := f.GetReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get streaming file reader")
	}

	var streamSize int64

	u.Progress.HashingFile(relativePath)

	defer func() {
		reader.Close() //nolint:errcheck
		u.Progress.FinishedHashingFile(relativePath, streamSize)
		u.Progress.FinishedFile(relativePath, ret)
	}()

	comp := pol.CompressionPolicy.CompressorForFile(f)
	metadataComp := pol.MetadataCompressionPolicy.MetadataCompressor()

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description:        "STREAMFILE:" + f.Name(),
		Compressor:         comp,
		MetadataCompressor: metadataComp,
		Splitter:           pol.SplitterPolicy.SplitterForFile(f),
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

func (u *Uploader) copyWithProgress(dst io.Writer, src io.Reader) (int64, error) {
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
				u.totalWrittenBytes.Add(int64(wroteBytes))
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
func (u *Uploader) uploadFileWithCheckpointing(ctx context.Context, relativePath string, file fs.File, pol *policy.Policy, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
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
func (u *Uploader) checkpointRoot(ctx context.Context, cp *checkpointRegistry, prototypeManifest *snapshot.Manifest) error {
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

	uploadLog(ctx).Debugf("checkpointed root %v", rootEntry.ObjectID)

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

// periodicallyCheckpoint periodically (every CheckpointInterval) invokes checkpointRoot until the
// returned cancellation function has been called.
func (u *Uploader) periodicallyCheckpoint(ctx context.Context, cp *checkpointRegistry, prototypeManifest *snapshot.Manifest) (cancelFunc func()) {
	shutdown := make(chan struct{})
	ch := u.getTicker(u.CheckpointInterval)

	go func() {
		for {
			select {
			case <-shutdown:
				return

			case <-ch:
				if err := u.checkpointRoot(ctx, cp, prototypeManifest); err != nil {
					uploadLog(ctx).Errorf("error checkpointing: %v", err)
					u.Cancel()

					return
				}

				// test action
				if u.checkpointFinished != nil {
					u.checkpointFinished <- struct{}{}
				}
			}
		}
	}()

	return func() {
		close(shutdown)
	}
}

// uploadDirWithCheckpointing uploads the specified Directory to the repository.
func (u *Uploader) uploadDirWithCheckpointing(ctx context.Context, rootDir fs.Directory, policyTree *policy.Tree, previousDirs []fs.Directory, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
	var (
		dmb DirManifestBuilder
		cp  checkpointRegistry
	)

	cancelCheckpointer := u.periodicallyCheckpoint(ctx, &cp, &snapshot.Manifest{Source: sourceInfo})
	defer cancelCheckpointer()

	var hc actionContext

	localDirPathOrEmpty := rootDir.LocalFilesystemPath()

	overrideDir, err := u.executeBeforeFolderAction(ctx, "before-snapshot-root", policyTree.EffectivePolicy().Actions.BeforeSnapshotRoot, localDirPathOrEmpty, &hc)
	if err != nil {
		return nil, dirReadError{errors.Wrap(err, "error executing before-snapshot-root action")}
	}

	defer u.executeAfterFolderAction(ctx, "after-snapshot-root", policyTree.EffectivePolicy().Actions.AfterSnapshotRoot, localDirPathOrEmpty, &hc)

	p := &policyTree.EffectivePolicy().OSSnapshotPolicy

	switch mode := osSnapshotMode(p); mode {
	case policy.OSSnapshotNever:
	case policy.OSSnapshotAlways, policy.OSSnapshotWhenAvailable:
		if overrideDir != nil {
			rootDir = overrideDir
		}

		switch osSnapshotDir, cleanup, err := createOSSnapshot(ctx, rootDir, p); {
		case err == nil:
			defer cleanup()

			overrideDir = osSnapshotDir

		case mode == policy.OSSnapshotWhenAvailable:
			uploadLog(ctx).Warnf("OS file system snapshot failed (ignoring): %v", err)
		default:
			return nil, dirReadError{errors.Wrap(err, "error creating OS file system snapshot")}
		}
	}

	if overrideDir != nil {
		rootDir = u.wrapIgnorefs(uploadLog(ctx), overrideDir, policyTree, true)
	}

	return uploadDirInternal(ctx, u, rootDir, policyTree, previousDirs, localDirPathOrEmpty, ".", &dmb, &cp)
}

type uploadWorkItem struct {
	err error
}

func rootCauseError(err error) error {
	var dre dirReadError

	if errors.As(err, &dre) {
		return rootCauseError(dre.error)
	}

	err = errors.Cause(err)

	var oserr *os.PathError
	if errors.As(err, &oserr) {
		err = oserr.Err
	}

	return err
}

func isDir(e *snapshot.DirEntry) bool {
	return e.Type == snapshot.EntryTypeDirectory
}

func (u *Uploader) processChildren(
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
			uploadLog(ctx).Debugw(
				"cache miss",
				"path", entryRelativePath,
				"mode", missedEntry.Mode().String(),
				"size", missedEntry.Size(),
				"mtime", missedEntry.ModTime())
		}
	}

	return nil
}

func (u *Uploader) maybeIgnoreCachedEntry(ctx context.Context, ent fs.Entry) fs.Entry {
	if h, ok := ent.(object.HasObjectID); ok {
		if 100*rand.Float64() < u.ForceHashPercentage { //nolint:gosec
			uploadLog(ctx).Debugw("re-hashing cached object", "oid", h.ObjectID())
			return nil
		}

		return ent
	}

	return nil
}

func (u *Uploader) effectiveParallelFileReads(pol *policy.Policy) int {
	p := u.ParallelUploads
	if p > 0 {
		// command-line override takes precedence.
		return p
	}

	// use policy setting or number of CPUs.
	maxParallelism := pol.UploadPolicy.MaxParallelFileReads.OrDefault(runtime.NumCPU())
	if p < 1 || p > maxParallelism {
		return maxParallelism
	}

	return p
}

func (u *Uploader) processDirectoryEntries(
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
	iter, err := dir.Iterate(ctx)
	if err != nil {
		return dirReadError{err}
	}

	defer iter.Close()

	entry, err := iter.Next(ctx)

	for entry != nil {
		entry2 := entry

		if u.IsCanceled() {
			return errCanceled
		}

		entryRelativePath := path.Join(dirRelativePath, entry2.Name())

		if wg.CanShareWork(u.workerPool) {
			wg.RunAsync(u.workerPool, func(_ *workshare.Pool[*uploadWorkItem], wi *uploadWorkItem) {
				wi.err = u.processSingle(ctx, entry2, entryRelativePath, parentDirBuilder, policyTree, prevDirs, localDirPathOrEmpty, parentCheckpointRegistry)
			}, &uploadWorkItem{})
		} else {
			if err2 := u.processSingle(ctx, entry2, entryRelativePath, parentDirBuilder, policyTree, prevDirs, localDirPathOrEmpty, parentCheckpointRegistry); err2 != nil {
				return err2
			}
		}

		entry, err = iter.Next(ctx)
	}

	if err != nil {
		return dirReadError{err}
	}

	return nil
}

//nolint:funlen
func (u *Uploader) processSingle(
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
		childTree := policyTree.Child(entry.Name())
		de, err := u.uploadSymlinkInternal(ctx, entryRelativePath, entry, childTree.EffectivePolicy().MetadataCompressionPolicy.MetadataCompressor())

		return u.processEntryUploadResult(ctx, de, err, entryRelativePath, parentDirBuilder,
			policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreFileErrors.OrDefault(false),
			u.OverrideEntryLogDetail.OrDefault(policyTree.EffectivePolicy().LoggingPolicy.Entries.Snapshotted.OrDefault(policy.LogDetailNone)),
			"snapshotted symlink", t0)

	case fs.File:
		atomic.AddInt32(&u.stats.NonCachedFiles, 1)

		de, err := u.uploadFileInternal(ctx, parentCheckpointRegistry, entryRelativePath, entry, policyTree.Child(entry.Name()).EffectivePolicy())

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

//nolint:unparam
func (u *Uploader) processEntryUploadResult(ctx context.Context, de *snapshot.DirEntry, err error, entryRelativePath string, parentDirBuilder *DirManifestBuilder, isIgnored bool, logDetail policy.LogDetail, logMessage string, t0 timetrack.Timer) error {
	if err != nil {
		u.reportErrorAndMaybeCancel(err, isIgnored, parentDirBuilder, entryRelativePath)
	} else {
		parentDirBuilder.AddEntry(de)
	}

	maybeLogEntryProcessed(
		uploadLog(ctx),
		logDetail,
		logMessage, entryRelativePath, de, err, t0)

	return nil
}

func uniqueChildDirectories(ctx context.Context, dirs []fs.Directory, childName string) []fs.Directory {
	var result []fs.Directory

	for _, d := range dirs {
		if child, err := d.Child(ctx, childName); err == nil {
			if sd, ok := child.(fs.Directory); ok {
				result = append(result, sd)
			}
		}
	}

	return uniqueDirectories(result)
}

func maybeLogEntryProcessed(logger logging.Logger, level policy.LogDetail, msg, relativePath string, de *snapshot.DirEntry, err error, timer timetrack.Timer) {
	if level <= policy.LogDetailNone && err == nil {
		return
	}

	var (
		bitsBuf       [10]interface{}
		keyValuePairs = append(bitsBuf[:0], "path", relativePath)
	)

	if err != nil {
		keyValuePairs = append(keyValuePairs, "error", err.Error())
	}

	if level >= minDetailLevelDuration {
		keyValuePairs = append(keyValuePairs, "dur", timer.Elapsed())
	}

	//nolint:nestif
	if de != nil {
		if level >= minDetailLevelSize {
			if ds := de.DirSummary; ds != nil {
				keyValuePairs = append(keyValuePairs, "size", ds.TotalFileSize)
			} else {
				keyValuePairs = append(keyValuePairs, "size", de.FileSize)
			}
		}

		if level >= minDetailLevelDirStats {
			if ds := de.DirSummary; ds != nil {
				keyValuePairs = append(keyValuePairs,
					"files", ds.TotalFileCount,
					"dirs", ds.TotalDirCount,
					"errors", ds.IgnoredErrorCount+ds.FatalErrorCount,
				)
			}
		}

		if level >= minDetailLevelModTime {
			if ds := de.DirSummary; ds != nil {
				keyValuePairs = append(keyValuePairs,
					"mtime", ds.MaxModTime.Format(time.RFC3339),
				)
			} else {
				keyValuePairs = append(keyValuePairs,
					"mtime", de.ModTime.Format(time.RFC3339),
				)
			}
		}

		if level >= minDetailLevelOID {
			keyValuePairs = append(keyValuePairs, "oid", de.ObjectID)
		}
	}

	logger.Debugw(msg, keyValuePairs...)
}

func uniqueDirectories(dirs []fs.Directory) []fs.Directory {
	if len(dirs) <= 1 {
		return dirs
	}

	unique := map[object.ID]fs.Directory{}

	for _, dir := range dirs {
		if hoid, ok := dir.(object.HasObjectID); ok {
			unique[hoid.ObjectID()] = dir
		}
	}

	if len(unique) == len(dirs) {
		return dirs
	}

	var result []fs.Directory
	for _, d := range unique {
		result = append(result, d)
	}

	return result
}

// dirReadError distinguishes an error thrown when attempting to read a directory.
type dirReadError struct {
	error
}

func uploadShallowDirInternal(ctx context.Context, directory fs.Directory, u *Uploader) (*snapshot.DirEntry, error) {
	if pf, ok := directory.(snapshot.HasDirEntryOrNil); ok {
		if de, err := pf.DirEntryOrNil(ctx); err != nil {
			return nil, errors.Wrapf(err, "error reading placeholder for %q", directory.Name())
		} else if de != nil {
			if _, err := u.repo.VerifyObject(ctx, de.ObjectID); err != nil {
				return nil, errors.Wrapf(err, "invalid placeholder for %q contains foreign object.ID", directory.Name())
			}

			return de, nil
		}
	}
	// No placeholder file exists, proceed as before.
	return nil, nil
}

func uploadDirInternal(
	ctx context.Context,
	u *Uploader,
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
			uploadLog(ctx),
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
		directory = u.wrapIgnorefs(uploadLog(ctx), overrideDir, policyTree, true)
	}

	if de, err := uploadShallowDirInternal(ctx, directory, u); de != nil || err != nil {
		return de, err
	}

	childCheckpointRegistry := &checkpointRegistry{}

	metadataComp := policyTree.EffectivePolicy().MetadataCompressionPolicy.MetadataCompressor()

	thisCheckpointRegistry.addCheckpointCallback(directory.Name(), func() (*snapshot.DirEntry, error) {
		// when snapshotting the parent, snapshot all our children and tell them to populate
		// childCheckpointBuilder
		thisCheckpointBuilder := thisDirBuilder.Clone()

		// invoke all child checkpoints which will populate thisCheckpointBuilder.
		if err := childCheckpointRegistry.runCheckpoints(thisCheckpointBuilder); err != nil {
			return nil, errors.Wrapf(err, "error checkpointing children")
		}

		checkpointManifest := thisCheckpointBuilder.Build(fs.UTCTimestampFromTime(directory.ModTime()), IncompleteReasonCheckpoint)

		oid, err := writeDirManifest(ctx, u.repo, dirRelativePath, checkpointManifest, metadataComp)
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

	oid, err := writeDirManifest(ctx, u.repo, dirRelativePath, dirManifest, metadataComp)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing dir manifest: %v", directory.Name())
	}

	return newDirEntryWithSummary(directory, oid, dirManifest.Summary)
}

func (u *Uploader) reportErrorAndMaybeCancel(err error, isIgnored bool, dmb *DirManifestBuilder, entryRelativePath string) {
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

// NewUploader creates new Uploader object for a given repository.
func NewUploader(r repo.RepositoryWriter) *Uploader {
	return &Uploader{
		repo:               r,
		Progress:           &NullUploadProgress{},
		EnableActions:      r.ClientOptions().EnableActions,
		CheckpointInterval: DefaultCheckpointInterval,
		getTicker:          time.Tick,
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Uploader) Cancel() {
	u.isCanceled.Store(true)
}

func (u *Uploader) maybeOpenDirectoryFromManifest(ctx context.Context, man *snapshot.Manifest) fs.Directory {
	if man == nil {
		return nil
	}

	ent := EntryFromDirEntry(u.repo, man.RootEntry)

	dir, ok := ent.(fs.Directory)
	if !ok {
		uploadLog(ctx).Debugf("previous manifest root is not a directory (was %T %+v)", ent, man.RootEntry)
		return nil
	}

	return dir
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and returns snapshot.Manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up uploads by utilizing hash cache.
func (u *Uploader) Upload(
	ctx context.Context,
	source fs.Entry,
	policyTree *policy.Tree,
	sourceInfo snapshot.SourceInfo,
	previousManifests ...*snapshot.Manifest,
) (*snapshot.Manifest, error) {
	ctx, span := uploadTracer.Start(ctx, "Upload")
	defer span.End()

	u.traceEnabled = span.IsRecording()

	u.Progress.UploadStarted()
	defer u.Progress.UploadFinished()

	if u.CheckpointInterval > DefaultCheckpointInterval {
		return nil, errors.Errorf("checkpoint interval cannot be greater than %v", DefaultCheckpointInterval)
	}

	parallel := u.effectiveParallelFileReads(policyTree.EffectivePolicy())

	uploadLog(ctx).Debugw("uploading", "source", sourceInfo, "previousManifests", len(previousManifests), "parallel", parallel)

	s := &snapshot.Manifest{
		Source: sourceInfo,
	}

	u.workerPool = workshare.NewPool[*uploadWorkItem](parallel - 1)
	defer u.workerPool.Close()

	u.stats = &snapshot.Stats{}
	u.totalWrittenBytes.Store(0)

	var err error

	s.StartTime = fs.UTCTimestampFromTime(u.repo.Time())

	switch entry := source.(type) {
	case fs.Directory:
		s.RootEntry, err = u.uploadDir(ctx, previousManifests, entry, policyTree, sourceInfo)

	case fs.File:
		u.Progress.EstimatedDataSize(1, entry.Size())
		s.RootEntry, err = u.uploadFileWithCheckpointing(ctx, entry.Name(), entry, policyTree.EffectivePolicy(), sourceInfo)

	default:
		return nil, errors.Errorf("unsupported source: %v", s.Source)
	}

	if err != nil {
		return nil, rootCauseError(err)
	}

	s.IncompleteReason = u.incompleteReason()
	s.EndTime = fs.UTCTimestampFromTime(u.repo.Time())
	s.Stats = *u.stats

	return s, nil
}

func (u *Uploader) uploadDir(
	ctx context.Context,
	previousManifests []*snapshot.Manifest,
	entry fs.Directory,
	policyTree *policy.Tree,
	sourceInfo snapshot.SourceInfo,
) (*snapshot.DirEntry, error) {
	var previousDirs []fs.Directory

	for _, m := range previousManifests {
		if d := u.maybeOpenDirectoryFromManifest(ctx, m); d != nil {
			previousDirs = append(previousDirs, d)
		}
	}

	estimationCtl := u.startDataSizeEstimation(ctx, entry, policyTree)
	defer func() {
		estimationCtl.Cancel()
		estimationCtl.Wait()
	}()

	wrapped := u.wrapIgnorefs(uploadLog(ctx), entry, policyTree, true /* reportIgnoreStats */)

	return u.uploadDirWithCheckpointing(ctx, wrapped, policyTree, previousDirs, sourceInfo)
}

func (u *Uploader) startDataSizeEstimation(
	ctx context.Context,
	entry fs.Directory,
	policyTree *policy.Tree,
) EstimationController {
	logger := estimateLog(ctx)
	wrapped := u.wrapIgnorefs(logger, entry, policyTree, false /* reportIgnoreStats */)

	if u.disableEstimation || !u.Progress.Enabled() {
		logger.Debug("Estimation disabled")
		return noOpEstimationCtrl
	}

	estimator := NewEstimator(wrapped, policyTree, u.Progress.EstimationParameters(), logger)

	estimator.StartEstimation(ctx, func(filesCount, totalFileSize int64) {
		u.Progress.EstimatedDataSize(filesCount, totalFileSize)
	})

	return estimator
}

func (u *Uploader) wrapIgnorefs(logger logging.Logger, entry fs.Directory, policyTree *policy.Tree, reportIgnoreStats bool) fs.Directory {
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
