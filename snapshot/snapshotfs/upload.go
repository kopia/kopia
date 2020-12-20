package snapshotfs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// DefaultCheckpointInterval is the default frequency of mid-upload checkpointing.
const DefaultCheckpointInterval = 45 * time.Minute

const copyBufferSize = 128 * 1024

var log = logging.GetContextLoggerFunc("snapshotfs")

var errCanceled = errors.New("canceled")

// reasons why a snapshot is incomplete.
const (
	IncompleteReasonCheckpoint   = "checkpoint"
	IncompleteReasonCanceled     = "canceled"
	IncompleteReasonLimitReached = "limit reached"
)

// Uploader supports efficient uploading files and directories to repository.
type Uploader struct {
	// values aligned to 8-bytes due to atomic access
	totalWrittenBytes int64

	Progress UploadProgress

	// automatically cancel the Upload after certain number of bytes
	MaxUploadBytes int64

	// ignore read errors
	IgnoreReadErrors bool

	// probability with cached entries will be ignored, must be [0..100]
	// 0=always use cached object entries if possible
	// 100=never use cached entries
	ForceHashPercentage int

	// Number of files to hash and upload in parallel.
	ParallelUploads int

	// Enable snapshot actions
	EnableActions bool

	// How frequently to create checkpoint snapshot entries.
	CheckpointInterval time.Duration

	repo repo.Repository

	// stats must be allocated on heap to enforce 64-bit alignment due to atomic access on ARM.
	stats    *snapshot.Stats
	canceled int32

	uploadBufPool sync.Pool

	getTicker func(time.Duration) <-chan time.Time

	// for testing only, when set will write to a given channel whenever checkpoint completes
	checkpointFinished chan struct{}

	// disable snapshot size estimation
	disableEstimation bool
}

// IsCanceled returns true if the upload is canceled.
func (u *Uploader) IsCanceled() bool {
	return u.incompleteReason() != ""
}

//
func (u *Uploader) incompleteReason() string {
	if c := atomic.LoadInt32(&u.canceled) != 0; c {
		return IncompleteReasonCanceled
	}

	wb := atomic.LoadInt64(&u.totalWrittenBytes)
	if mub := u.MaxUploadBytes; mub > 0 && wb > mub {
		return IncompleteReasonLimitReached
	}

	return ""
}

func (u *Uploader) uploadFileInternal(ctx context.Context, parentCheckpointRegistry *checkpointRegistry, relativePath string, f fs.File, pol *policy.Policy, asyncWrites int) (*snapshot.DirEntry, error) {
	u.Progress.HashingFile(relativePath)
	defer u.Progress.FinishedHashingFile(relativePath, f.Size())

	file, err := f.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open file")
	}
	defer file.Close() //nolint:errcheck

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "FILE:" + f.Name(),
		Compressor:  pol.CompressionPolicy.CompressorForFile(f),
		AsyncWrites: asyncWrites,
	})
	defer writer.Close() //nolint:errcheck

	parentCheckpointRegistry.addCheckpointCallback(f, func() (*snapshot.DirEntry, error) {
		// nolint:govet
		checkpointID, err := writer.Checkpoint()
		if err != nil {
			return nil, errors.Wrap(err, "checkpoint error")
		}

		if checkpointID == "" {
			return nil, nil
		}

		return newDirEntry(f, checkpointID)
	})

	defer parentCheckpointRegistry.removeCheckpointCallback(f)

	written, err := u.copyWithProgress(writer, file, 0, f.Size())
	if err != nil {
		return nil, err
	}

	fi2, err := file.Entry()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get file entry after copying")
	}

	r, err := writer.Result()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get result")
	}

	de, err := newDirEntry(fi2, r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written

	atomic.AddInt32(&u.stats.TotalFileCount, 1)
	atomic.AddInt64(&u.stats.TotalFileSize, de.FileSize)

	return de, nil
}

func (u *Uploader) uploadSymlinkInternal(ctx context.Context, relativePath string, f fs.Symlink) (*snapshot.DirEntry, error) {
	u.Progress.HashingFile(relativePath)
	defer u.Progress.FinishedHashingFile(relativePath, f.Size())

	target, err := f.Readlink(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read symlink")
	}

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "SYMLINK:" + f.Name(),
	})
	defer writer.Close() //nolint:errcheck

	written, err := u.copyWithProgress(writer, bytes.NewBufferString(target), 0, f.Size())
	if err != nil {
		return nil, err
	}

	r, err := writer.Result()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get result")
	}

	de, err := newDirEntry(f, r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written

	return de, nil
}

func (u *Uploader) copyWithProgress(dst io.Writer, src io.Reader, completed, length int64) (int64, error) {
	uploadBufPtr := u.uploadBufPool.Get().(*[]byte)
	defer u.uploadBufPool.Put(uploadBufPtr)

	uploadBuf := *uploadBufPtr

	var written int64

	for {
		if u.IsCanceled() {
			return 0, errors.Wrap(errCanceled, "canceled when copying data")
		}

		readBytes, readErr := src.Read(uploadBuf)

		// nolint:nestif
		if readBytes > 0 {
			wroteBytes, writeErr := dst.Write(uploadBuf[0:readBytes])
			if wroteBytes > 0 {
				written += int64(wroteBytes)
				completed += int64(wroteBytes)
				atomic.AddInt64(&u.totalWrittenBytes, int64(wroteBytes))
				u.Progress.HashedBytes(int64(wroteBytes))

				if length < completed {
					length = completed
				}
			}

			if writeErr != nil {
				// nolint:wrapcheck
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

			// nolint:wrapcheck
			return written, readErr
		}
	}

	return written, nil
}

func newDirEntryWithSummary(d fs.Entry, oid object.ID, summ *fs.DirectorySummary) (*snapshot.DirEntry, error) {
	de, err := newDirEntry(d, oid)
	if err != nil {
		return nil, err
	}

	de.DirSummary = summ

	return de, nil
}

func newDirEntry(md fs.Entry, oid object.ID) (*snapshot.DirEntry, error) {
	var entryType snapshot.EntryType

	switch md := md.(type) {
	case fs.Directory:
		entryType = snapshot.EntryTypeDirectory
	case fs.Symlink:
		entryType = snapshot.EntryTypeSymlink
	case fs.File:
		entryType = snapshot.EntryTypeFile
	default:
		return nil, errors.Errorf("invalid entry type %T", md)
	}

	return &snapshot.DirEntry{
		Name:        md.Name(),
		Type:        entryType,
		Permissions: snapshot.Permissions(md.Mode() & os.ModePerm),
		FileSize:    md.Size(),
		ModTime:     md.ModTime(),
		UserID:      md.Owner().UserID,
		GroupID:     md.Owner().GroupID,
		ObjectID:    oid,
	}, nil
}

// uploadFileWithCheckpointing uploads the specified File to the repository.
func (u *Uploader) uploadFileWithCheckpointing(ctx context.Context, relativePath string, file fs.File, pol *policy.Policy, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
	par := u.effectiveParallelUploads()
	if par == 1 {
		par = 0
	}

	var cp checkpointRegistry

	cancelCheckpointer := u.periodicallyCheckpoint(ctx, &cp, &snapshot.Manifest{Source: sourceInfo})
	defer cancelCheckpointer()

	res, err := u.uploadFileInternal(ctx, &cp, relativePath, file, pol, par)
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
	var dmbCheckpoint dirManifestBuilder
	if err := cp.runCheckpoints(&dmbCheckpoint); err != nil {
		return errors.Wrap(err, "running checkpointers")
	}

	checkpointManifest := dmbCheckpoint.Build(u.repo.Time(), "dummy")
	if len(checkpointManifest.Entries) == 0 {
		// did not produce a checkpoint, that's ok
		return nil
	}

	if len(checkpointManifest.Entries) > 1 {
		return errors.Errorf("produced more than one checkpoint: %v", len(checkpointManifest.Entries))
	}

	rootEntry := checkpointManifest.Entries[0]

	log(ctx).Debugf("checkpointed root %v", rootEntry.ObjectID)

	man := *prototypeManifest
	man.RootEntry = rootEntry
	man.EndTime = u.repo.Time()
	man.StartTime = man.EndTime
	man.IncompleteReason = IncompleteReasonCheckpoint

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
// returned cancelation function has been called.
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
					log(ctx).Warningf("error checkpointing: %v", err)
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
		dmb dirManifestBuilder
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

	if overrideDir != nil {
		rootDir = overrideDir
	}

	defer u.executeAfterFolderAction(ctx, "after-snapshot-root", policyTree.EffectivePolicy().Actions.AfterSnapshotRoot, localDirPathOrEmpty, &hc)

	return uploadDirInternal(ctx, u, rootDir, policyTree, previousDirs, localDirPathOrEmpty, ".", &dmb, &cp)
}

func (u *Uploader) foreachEntryUnlessCanceled(ctx context.Context, parallel int, relativePath string, entries fs.Entries, cb func(ctx context.Context, entry fs.Entry, entryRelativePath string) error) error {
	if parallel > len(entries) {
		// don't launch more goroutines than needed
		parallel = len(entries)
	}

	if parallel == 0 {
		return nil
	}

	ch := make(chan fs.Entry)
	eg, ctx := errgroup.WithContext(ctx)

	// one goroutine to pump entries into channel until ctx is closed.
	eg.Go(func() error {
		defer close(ch)

		for _, e := range entries {
			select {
			case ch <- e: // sent to channel
			case <-ctx.Done(): // context closed
				return nil
			}
		}
		return nil
	})

	// launch N workers in parallel
	for i := 0; i < parallel; i++ {
		eg.Go(func() error {
			for entry := range ch {
				if u.IsCanceled() {
					// nolint:wrapcheck
					return errCanceled
				}

				entryRelativePath := path.Join(relativePath, entry.Name())
				if err := cb(ctx, entry, entryRelativePath); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func rootCauseError(err error) error {
	err = errors.Cause(err)

	var oserr *os.PathError
	if errors.As(err, &oserr) {
		err = oserr.Err
	}

	return err
}

type dirManifestBuilder struct {
	mu sync.Mutex

	summary fs.DirectorySummary
	entries []*snapshot.DirEntry
}

// Clone clones the current state of dirManifestBuilder.
func (b *dirManifestBuilder) Clone() *dirManifestBuilder {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &dirManifestBuilder{
		summary: b.summary.Clone(),
		entries: append([]*snapshot.DirEntry(nil), b.entries...),
	}
}

func (b *dirManifestBuilder) addEntry(de *snapshot.DirEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = append(b.entries, de)

	if de.ModTime.After(b.summary.MaxModTime) {
		b.summary.MaxModTime = de.ModTime
	}

	// nolint:exhaustive
	switch de.Type {
	case snapshot.EntryTypeSymlink:
		b.summary.TotalSymlinkCount++

	case snapshot.EntryTypeFile:
		b.summary.TotalFileCount++
		b.summary.TotalFileSize += de.FileSize

	case snapshot.EntryTypeDirectory:
		if childSummary := de.DirSummary; childSummary != nil {
			b.summary.TotalFileCount += childSummary.TotalFileCount
			b.summary.TotalFileSize += childSummary.TotalFileSize
			b.summary.TotalDirCount += childSummary.TotalDirCount
			b.summary.NumFailed += childSummary.NumFailed
			b.summary.FailedEntries = append(b.summary.FailedEntries, childSummary.FailedEntries...)

			if childSummary.MaxModTime.After(b.summary.MaxModTime) {
				b.summary.MaxModTime = childSummary.MaxModTime
			}
		}
	}
}

func (b *dirManifestBuilder) addFailedEntry(relPath string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.summary.NumFailed++
	b.summary.FailedEntries = append(b.summary.FailedEntries, &fs.EntryWithError{
		EntryPath: relPath,
		Error:     err.Error(),
	})
}

func (b *dirManifestBuilder) Build(dirModTime time.Time, incompleteReason string) *snapshot.DirManifest {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.summary
	s.TotalDirCount++

	if len(b.entries) == 0 {
		s.MaxModTime = dirModTime
	}

	s.IncompleteReason = incompleteReason

	// take top N sorted failed entries
	if len(b.summary.FailedEntries) > 0 {
		sort.Slice(b.summary.FailedEntries, func(i, j int) bool {
			return b.summary.FailedEntries[i].EntryPath < b.summary.FailedEntries[j].EntryPath
		})

		if len(b.summary.FailedEntries) > fs.MaxFailedEntriesPerDirectorySummary {
			b.summary.FailedEntries = b.summary.FailedEntries[0:fs.MaxFailedEntriesPerDirectorySummary]
		}
	}

	// sort the result, directories first, then non-directories, ordered by name
	sort.Slice(b.entries, func(i, j int) bool {
		if leftDir, rightDir := isDir(b.entries[i]), isDir(b.entries[j]); leftDir != rightDir {
			// directories get sorted before non-directories
			return leftDir
		}

		return b.entries[i].Name < b.entries[j].Name
	})

	return &snapshot.DirManifest{
		StreamType: directoryStreamType,
		Summary:    &s,
		Entries:    b.entries,
	}
}

func isDir(e *snapshot.DirEntry) bool {
	return e.Type == snapshot.EntryTypeDirectory
}

func (u *Uploader) processChildren(
	ctx context.Context,
	parentDirCheckpointRegistry *checkpointRegistry,
	parentDirBuilder *dirManifestBuilder,
	localDirPathOrEmpty, relativePath string,
	entries fs.Entries,
	policyTree *policy.Tree,
	previousEntries []fs.Entries,
) error {
	if err := u.processSubdirectories(ctx, parentDirCheckpointRegistry, parentDirBuilder, localDirPathOrEmpty, relativePath, entries, policyTree, previousEntries); err != nil {
		return err
	}

	if err := u.processNonDirectories(ctx, parentDirCheckpointRegistry, parentDirBuilder, relativePath, entries, policyTree, previousEntries); err != nil {
		return err
	}

	return nil
}

func (u *Uploader) processSubdirectories(
	ctx context.Context,
	parentDirCheckpointRegistry *checkpointRegistry,
	parentDirBuilder *dirManifestBuilder,
	localDirPathOrEmpty, relativePath string,
	entries fs.Entries,
	policyTree *policy.Tree,
	previousEntries []fs.Entries,
) error {
	// for now don't process subdirectories in parallel, we need a mechanism to
	// prevent explosion of parallelism
	const parallelism = 1

	return u.foreachEntryUnlessCanceled(ctx, parallelism, relativePath, entries, func(ctx context.Context, entry fs.Entry, entryRelativePath string) error {
		dir, ok := entry.(fs.Directory)
		if !ok {
			// skip non-directories
			return nil
		}

		var previousDirs []fs.Directory
		for _, e := range previousEntries {
			if d, _ := e.FindByName(entry.Name()).(fs.Directory); d != nil {
				previousDirs = append(previousDirs, d)
			}
		}

		previousDirs = uniqueDirectories(previousDirs)

		childDirBuilder := &dirManifestBuilder{}

		childLocalDirPathOrEmpty := ""
		if localDirPathOrEmpty != "" {
			childLocalDirPathOrEmpty = filepath.Join(localDirPathOrEmpty, entry.Name())
		}

		de, err := uploadDirInternal(ctx, u, dir, policyTree.Child(entry.Name()), previousDirs, childLocalDirPathOrEmpty, entryRelativePath, childDirBuilder, parentDirCheckpointRegistry)
		if errors.Is(err, errCanceled) {
			return err
		}

		if err != nil {
			// Note: This only catches errors in subdirectories of the snapshot root, not on the snapshot
			// root itself. The intention is to always fail if the top level directory can't be read,
			// otherwise a meaningless, empty snapshot is created that can't be restored.
			ignoreDirErr := u.shouldIgnoreDirectoryReadErrors(policyTree)

			var dre dirReadError
			if errors.As(err, &dre) && ignoreDirErr {
				rc := rootCauseError(dre.error)

				u.Progress.IgnoredError(entryRelativePath, rc)
				parentDirBuilder.addFailedEntry(entryRelativePath, rc)
				return nil
			}

			return errors.Errorf("unable to process directory %q: %s", entry.Name(), err)
		}

		parentDirBuilder.addEntry(de)

		return nil
	})
}

func metadataEquals(e1, e2 fs.Entry) bool {
	if l, r := e1.ModTime(), e2.ModTime(); !l.Equal(r) {
		return false
	}

	if l, r := e1.Mode(), e2.Mode(); l != r {
		return false
	}

	if l, r := e1.Size(), e2.Size(); l != r {
		return false
	}

	if l, r := e1.Owner(), e2.Owner(); l != r {
		return false
	}

	return true
}

func findCachedEntry(ctx context.Context, entry fs.Entry, prevEntries []fs.Entries) fs.Entry {
	for _, e := range prevEntries {
		if ent := e.FindByName(entry.Name()); ent != nil {
			if metadataEquals(entry, ent) {
				return ent
			}

			log(ctx).Debugf("found non-matching entry for %v: %v %v %v", entry.Name(), ent.Mode(), ent.Size(), ent.ModTime())
		}
	}

	log(ctx).Debugf("could not find cache entry for %v", entry.Name())

	return nil
}

func (u *Uploader) maybeIgnoreCachedEntry(ctx context.Context, ent fs.Entry) fs.Entry {
	if h, ok := ent.(object.HasObjectID); ok {
		if rand.Intn(100) < u.ForceHashPercentage { // nolint:gomnd,gosec
			log(ctx).Debugf("re-hashing cached object: %v", h.ObjectID())
			return nil
		}

		return ent
	}

	return nil
}

func (u *Uploader) effectiveParallelUploads() int {
	p := u.ParallelUploads
	if p == 0 {
		p = runtime.NumCPU()
	}

	return p
}

func (u *Uploader) processNonDirectories(ctx context.Context, parentCheckpointRegistry *checkpointRegistry, parentDirBuilder *dirManifestBuilder, dirRelativePath string, entries fs.Entries, policyTree *policy.Tree, prevEntries []fs.Entries) error {
	workerCount := u.effectiveParallelUploads()

	var asyncWritesPerFile int

	if len(entries) < workerCount {
		if len(entries) > 0 {
			asyncWritesPerFile = workerCount / len(entries)
			if asyncWritesPerFile == 1 {
				asyncWritesPerFile = 0
			}
		}

		workerCount = len(entries)
	}

	return u.foreachEntryUnlessCanceled(ctx, workerCount, dirRelativePath, entries, func(ctx context.Context, entry fs.Entry, entryRelativePath string) error {
		// note this function runs in parallel and updates 'u.stats', which must be done using atomic operations.
		if _, ok := entry.(fs.Directory); ok {
			// skip directories
			return nil
		}

		// See if we had this name during either of previous passes.
		if cachedEntry := u.maybeIgnoreCachedEntry(ctx, findCachedEntry(ctx, entry, prevEntries)); cachedEntry != nil {
			atomic.AddInt32(&u.stats.CachedFiles, 1)
			atomic.AddInt64(&u.stats.TotalFileSize, entry.Size())
			u.Progress.CachedFile(filepath.Join(dirRelativePath, entry.Name()), entry.Size())

			// compute entryResult now, cachedEntry is short-lived
			cachedDirEntry, err := newDirEntry(entry, cachedEntry.(object.HasObjectID).ObjectID())
			if err != nil {
				return errors.Wrap(err, "unable to create dir entry")
			}

			parentDirBuilder.addEntry(cachedDirEntry)
			return nil
		}

		switch entry := entry.(type) {
		case fs.Symlink:
			de, err := u.uploadSymlinkInternal(ctx, entryRelativePath, entry)
			if err != nil {
				return u.maybeIgnoreFileReadError(err, parentDirBuilder, entryRelativePath, policyTree)
			}

			parentDirBuilder.addEntry(de)
			return nil

		case fs.File:
			atomic.AddInt32(&u.stats.NonCachedFiles, 1)
			de, err := u.uploadFileInternal(ctx, parentCheckpointRegistry, entryRelativePath, entry, policyTree.Child(entry.Name()).EffectivePolicy(), asyncWritesPerFile)
			if err != nil {
				return u.maybeIgnoreFileReadError(err, parentDirBuilder, entryRelativePath, policyTree)
			}

			parentDirBuilder.addEntry(de)
			return nil

		default:
			return errors.Errorf("file type not supported: %v", entry.Mode())
		}
	})
}

func maybeReadDirectoryEntries(ctx context.Context, dir fs.Directory) fs.Entries {
	if dir == nil {
		return nil
	}

	ent, err := dir.Readdir(ctx)
	if err != nil {
		log(ctx).Warningf("unable to read previous directory entries: %v", err)
		return nil
	}

	return ent
}

func uniqueDirectories(dirs []fs.Directory) []fs.Directory {
	if len(dirs) <= 1 {
		return dirs
	}

	unique := map[object.ID]fs.Directory{}
	for _, dir := range dirs {
		unique[dir.(object.HasObjectID).ObjectID()] = dir
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

func uploadDirInternal(
	ctx context.Context,
	u *Uploader,
	directory fs.Directory,
	policyTree *policy.Tree,
	previousDirs []fs.Directory,
	localDirPathOrEmpty, dirRelativePath string,
	thisDirBuilder *dirManifestBuilder,
	thisCheckpointRegistry *checkpointRegistry,
) (*snapshot.DirEntry, error) {
	atomic.AddInt32(&u.stats.TotalDirectoryCount, 1)

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
		directory = overrideDir
	}

	t0 := u.repo.Time()
	entries, direrr := directory.Readdir(ctx)
	log(ctx).Debugf("finished reading directory %v in %v", dirRelativePath, u.repo.Time().Sub(t0))

	if direrr != nil {
		return nil, dirReadError{direrr}
	}

	var prevEntries []fs.Entries

	for _, d := range uniqueDirectories(previousDirs) {
		if ent := maybeReadDirectoryEntries(ctx, d); ent != nil {
			prevEntries = append(prevEntries, ent)
		}
	}

	childCheckpointRegistry := &checkpointRegistry{}

	thisCheckpointRegistry.addCheckpointCallback(directory, func() (*snapshot.DirEntry, error) {
		// when snapshotting the parent, snapshot all our children and tell them to populate
		// childCheckpointBuilder
		thisCheckpointBuilder := thisDirBuilder.Clone()

		// invoke all child checkpoints which will populate thisCheckpointBuilder.
		if err := childCheckpointRegistry.runCheckpoints(thisCheckpointBuilder); err != nil {
			return nil, errors.Wrapf(err, "error checkpointing children")
		}

		checkpointManifest := thisCheckpointBuilder.Build(directory.ModTime(), IncompleteReasonCheckpoint)
		oid, err := u.writeDirManifest(ctx, dirRelativePath, checkpointManifest)
		if err != nil {
			return nil, errors.Wrap(err, "error writing dir manifest")
		}

		return newDirEntryWithSummary(directory, oid, checkpointManifest.Summary)
	})
	defer thisCheckpointRegistry.removeCheckpointCallback(directory)

	if err := u.processChildren(ctx, childCheckpointRegistry, thisDirBuilder, localDirPathOrEmpty, dirRelativePath, entries, policyTree, prevEntries); err != nil && !errors.Is(err, errCanceled) {
		return nil, err
	}

	dirManifest := thisDirBuilder.Build(directory.ModTime(), u.incompleteReason())

	oid, err := u.writeDirManifest(ctx, dirRelativePath, dirManifest)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing dir manifest: %v", directory.Name())
	}

	return newDirEntryWithSummary(directory, oid, dirManifest.Summary)
}

func (u *Uploader) writeDirManifest(ctx context.Context, dirRelativePath string, dirManifest *snapshot.DirManifest) (object.ID, error) {
	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "DIR:" + dirRelativePath,
		Prefix:      objectIDPrefixDirectory,
	})

	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(dirManifest); err != nil {
		return "", errors.Wrap(err, "unable to encode directory JSON")
	}

	oid, err := writer.Result()
	if err != nil {
		return "", errors.Wrap(err, "unable to write directory")
	}

	return oid, nil
}

func (u *Uploader) maybeIgnoreFileReadError(err error, dmb *dirManifestBuilder, entryRelativePath string, policyTree *policy.Tree) error {
	errHandlingPolicy := policyTree.EffectivePolicy().ErrorHandlingPolicy

	if u.IgnoreReadErrors || errHandlingPolicy.IgnoreFileErrorsOrDefault(false) {
		err = rootCauseError(err)
		u.Progress.IgnoredError(entryRelativePath, err)
		dmb.addFailedEntry(entryRelativePath, err)

		return nil
	}

	return err
}

func (u *Uploader) shouldIgnoreDirectoryReadErrors(policyTree *policy.Tree) bool {
	errHandlingPolicy := policyTree.EffectivePolicy().ErrorHandlingPolicy

	if u.IgnoreReadErrors {
		return true
	}

	return errHandlingPolicy.IgnoreDirectoryErrorsOrDefault(false)
}

// NewUploader creates new Uploader object for a given repository.
func NewUploader(r repo.Repository) *Uploader {
	return &Uploader{
		repo:               r,
		Progress:           &NullUploadProgress{},
		IgnoreReadErrors:   false,
		ParallelUploads:    1,
		EnableActions:      r.ClientOptions().EnableActions,
		CheckpointInterval: DefaultCheckpointInterval,
		getTicker:          time.Tick,
		uploadBufPool: sync.Pool{
			New: func() interface{} {
				p := make([]byte, copyBufferSize)

				return &p
			},
		},
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Uploader) Cancel() {
	atomic.StoreInt32(&u.canceled, 1)
}

func (u *Uploader) maybeOpenDirectoryFromManifest(ctx context.Context, man *snapshot.Manifest) fs.Directory {
	if man == nil {
		return nil
	}

	ent, err := EntryFromDirEntry(u.repo, man.RootEntry)
	if err != nil {
		log(ctx).Warningf("invalid previous manifest root entry %v: %v", man.RootEntry, err)
		return nil
	}

	dir, ok := ent.(fs.Directory)
	if !ok {
		log(ctx).Debugf("previous manifest root is not a directory (was %T %+v)", ent, man.RootEntry)
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
	log(ctx).Debugf("Uploading %v", sourceInfo)

	s := &snapshot.Manifest{
		Source: sourceInfo,
	}

	u.Progress.UploadStarted()

	defer u.Progress.UploadFinished()

	u.stats = &snapshot.Stats{}
	u.totalWrittenBytes = 0

	var err error

	s.StartTime = u.repo.Time()

	var scanWG sync.WaitGroup

	scanctx, cancelScan := context.WithCancel(ctx)

	defer cancelScan()

	switch entry := source.(type) {
	case fs.Directory:
		var previousDirs []fs.Directory

		for _, m := range previousManifests {
			if d := u.maybeOpenDirectoryFromManifest(ctx, m); d != nil {
				previousDirs = append(previousDirs, d)
			}
		}

		entry = ignorefs.New(entry, policyTree, ignorefs.ReportIgnoredFiles(func(_ string, md fs.Entry) {
			u.stats.AddExcluded(md)
		}))

		scanWG.Add(1)

		go func() {
			defer scanWG.Done()

			ds, _ := u.scanDirectory(scanctx, entry)

			u.Progress.EstimatedDataSize(ds.numFiles, ds.totalFileSize)
		}()

		s.RootEntry, err = u.uploadDirWithCheckpointing(ctx, entry, policyTree, previousDirs, sourceInfo)

	case fs.File:
		u.Progress.EstimatedDataSize(1, entry.Size())
		s.RootEntry, err = u.uploadFileWithCheckpointing(ctx, entry.Name(), entry, policyTree.EffectivePolicy(), sourceInfo)

	default:
		return nil, errors.Errorf("unsupported source: %v", s.Source)
	}

	if err != nil {
		return nil, err
	}

	cancelScan()
	scanWG.Wait()

	s.IncompleteReason = u.incompleteReason()
	s.EndTime = u.repo.Time()
	s.Stats = *u.stats

	return s, nil
}
