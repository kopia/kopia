package snapshotfs

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"io"
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

	// How frequently to create checkpoint snapshot entries.
	CheckpointInterval time.Duration

	repo repo.Repository

	stats              snapshot.Stats
	canceled           int32
	nextCheckpointTime time.Time

	uploadBufPool sync.Pool
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

	if !u.nextCheckpointTime.IsZero() && u.repo.Time().After(u.nextCheckpointTime) {
		return IncompleteReasonCheckpoint
	}

	wb := atomic.LoadInt64(&u.totalWrittenBytes)
	if mub := u.MaxUploadBytes; mub > 0 && wb > mub {
		return IncompleteReasonLimitReached
	}

	return ""
}

func (u *Uploader) uploadFileInternal(ctx context.Context, relativePath string, f fs.File, pol *policy.Policy, asyncWrites int) (*snapshot.DirEntry, error) {
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

	written, err := u.copyWithProgress(writer, file, 0, f.Size())
	if err != nil {
		return nil, err
	}

	fi2, err := file.Entry()
	if err != nil {
		return nil, err
	}

	r, err := writer.Result()
	if err != nil {
		return nil, err
	}

	de, err := newDirEntry(fi2, r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.FileSize = written

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
		return nil, err
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
			return 0, errCanceled
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
				return written, writeErr
			}

			if readBytes != wroteBytes {
				return written, io.ErrShortWrite
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}

			return written, readErr
		}
	}

	return written, nil
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

// uploadFile uploads the specified File to the repository.
func (u *Uploader) uploadFile(ctx context.Context, relativePath string, file fs.File, pol *policy.Policy) (*snapshot.DirEntry, error) {
	par := u.effectiveParallelUploads()
	if par == 1 {
		par = 0
	}

	res, err := u.uploadFileInternal(ctx, relativePath, file, pol, par)
	if err != nil {
		return nil, err
	}

	de, err := newDirEntry(file, res.ObjectID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create dir entry")
	}

	de.DirSummary = &fs.DirectorySummary{
		TotalFileCount: 1,
		TotalFileSize:  res.FileSize,
		MaxModTime:     res.ModTime,
	}

	return de, nil
}

// uploadDirWithCheckpointing uploads the specified Directory to the repository.
func (u *Uploader) uploadDirWithCheckpointing(ctx context.Context, rootDir fs.Directory, policyTree *policy.Tree, previousDirs []fs.Directory, sourceInfo snapshot.SourceInfo) (*snapshot.DirEntry, error) {
	for {
		if u.CheckpointInterval != 0 {
			u.nextCheckpointTime = u.repo.Time().Add(u.CheckpointInterval)
		} else {
			u.nextCheckpointTime = time.Time{}
		}

		startTime := u.repo.Time()

		oid, summ, err := uploadDirInternal(ctx, u, rootDir, policyTree, previousDirs, ".")
		if err != nil && !errors.Is(err, errCanceled) {
			return nil, err
		}

		de, err := newDirEntry(rootDir, oid)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create dir entry")
		}

		de.DirSummary = &summ

		if summ.IncompleteReason == IncompleteReasonCheckpoint {
			u.Progress.Checkpoint()

			// when retrying use the partial snapshot
			previousDirs = append(previousDirs, DirectoryEntry(u.repo, oid, &summ))

			man := &snapshot.Manifest{
				StartTime:        startTime,
				EndTime:          u.repo.Time(),
				RootEntry:        de,
				Source:           sourceInfo,
				IncompleteReason: summ.IncompleteReason,
			}

			if _, err = snapshot.SaveSnapshot(ctx, u.repo, man); err != nil {
				return nil, errors.Wrap(err, "error saving checkpoint")
			}

			if err = u.repo.Flush(ctx); err != nil {
				return nil, errors.Wrap(err, "error flushing saving checkpoint")
			}

			continue
		}

		return de, err
	}
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

type dirEntryOrError struct {
	de          *snapshot.DirEntry
	failedEntry *fs.EntryWithError
}

func rootCauseError(err error) error {
	err = errors.Cause(err)
	if oserr, ok := err.(*os.PathError); ok {
		err = oserr.Err
	}

	return err
}

func (u *Uploader) populateChildEntries(parent *snapshot.DirManifest, children <-chan dirEntryOrError) {
	parentSummary := parent.Summary

	for it := range children {
		if it.failedEntry != nil {
			parentSummary.NumFailed++
			parentSummary.FailedEntries = append(parentSummary.FailedEntries, it.failedEntry)

			continue
		}

		de := it.de

		// nolint:exhaustive
		switch de.Type {
		case snapshot.EntryTypeFile:
			u.stats.TotalFileCount++
			u.stats.TotalFileSize += de.FileSize
			parentSummary.TotalFileCount++
			parentSummary.TotalFileSize += de.FileSize

			if de.ModTime.After(parentSummary.MaxModTime) {
				parentSummary.MaxModTime = de.ModTime
			}

		case snapshot.EntryTypeDirectory:
			if childSummary := de.DirSummary; childSummary != nil {
				parentSummary.TotalFileCount += childSummary.TotalFileCount
				parentSummary.TotalFileSize += childSummary.TotalFileSize
				parentSummary.TotalDirCount += childSummary.TotalDirCount
				parentSummary.NumFailed += childSummary.NumFailed
				parentSummary.FailedEntries = append(parentSummary.FailedEntries, childSummary.FailedEntries...)

				if childSummary.MaxModTime.After(parentSummary.MaxModTime) {
					parentSummary.MaxModTime = childSummary.MaxModTime
				}
			}
		}

		parent.Entries = append(parent.Entries, de)
	}

	// take top N sorted failed entries
	if len(parent.Summary.FailedEntries) > 0 {
		sort.Slice(parent.Summary.FailedEntries, func(i, j int) bool {
			return parent.Summary.FailedEntries[i].EntryPath < parent.Summary.FailedEntries[j].EntryPath
		})

		if len(parent.Summary.FailedEntries) > fs.MaxFailedEntriesPerDirectorySummary {
			parent.Summary.FailedEntries = parent.Summary.FailedEntries[0:fs.MaxFailedEntriesPerDirectorySummary]
		}
	}

	// sort the result, directories first, then non-directories, ordered by name
	sort.Slice(parent.Entries, func(i, j int) bool {
		if leftDir, rightDir := isDir(parent.Entries[i]), isDir(parent.Entries[j]); leftDir != rightDir {
			// directories get sorted before non-directories
			return leftDir
		}

		return parent.Entries[i].Name < parent.Entries[j].Name
	})
}

func isDir(e *snapshot.DirEntry) bool {
	return e.Type == snapshot.EntryTypeDirectory
}

func (u *Uploader) processChildren(ctx context.Context, dirManifest *snapshot.DirManifest, relativePath string, entries fs.Entries, policyTree *policy.Tree, previousEntries []fs.Entries) error {
	var wg sync.WaitGroup

	// channel where we will add directory and file entries, possibly in parallel
	output := make(chan dirEntryOrError)

	// goroutine that will drain data from 'output' and update dirManifest
	wg.Add(1)

	go func() {
		defer wg.Done()
		u.populateChildEntries(dirManifest, output)
	}()

	defer func() {
		// before this function returns, close the output channel and wait for the goroutine above to complete.
		close(output)
		wg.Wait()
	}()

	if err := u.processSubdirectories(ctx, output, relativePath, entries, policyTree, previousEntries); err != nil {
		return err
	}

	if err := u.processNonDirectories(ctx, output, relativePath, entries, policyTree, previousEntries); err != nil {
		return err
	}

	return nil
}

func (u *Uploader) processSubdirectories(ctx context.Context, output chan dirEntryOrError, relativePath string, entries fs.Entries, policyTree *policy.Tree, previousEntries []fs.Entries) error {
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

		oid, subdirsumm, err := uploadDirInternal(ctx, u, dir, policyTree.Child(entry.Name()), previousDirs, entryRelativePath)
		if errors.Is(err, errCanceled) {
			return err
		}

		if err != nil {
			// Note: This only catches errors in subdirectories of the snapshot root, not on the snapshot
			// root itself. The intention is to always fail if the top level directory can't be read,
			// otherwise a meaningless, empty snapshot is created that can't be restored.
			ignoreDirErr := u.shouldIgnoreDirectoryReadErrors(policyTree)
			if dre, ok := err.(dirReadError); ok && ignoreDirErr {
				rc := rootCauseError(dre.error)

				u.Progress.IgnoredError(entryRelativePath, rc)
				output <- dirEntryOrError{
					failedEntry: &fs.EntryWithError{
						EntryPath: entryRelativePath,
						Error:     rc.Error(),
					},
				}
				return nil
			}
			return errors.Errorf("unable to process directory %q: %s", entry.Name(), err)
		}

		de, err := newDirEntry(dir, oid)
		if err != nil {
			return errors.Wrap(err, "unable to create dir entry")
		}

		de.DirSummary = &subdirsumm
		output <- dirEntryOrError{de: de}
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

// objectIDPercent arbitrarily maps given object ID onto a number 0.99.
func objectIDPercent(obj object.ID) int {
	h := fnv.New32a()
	io.WriteString(h, obj.String()) //nolint:errcheck

	return int(h.Sum32() % 100) //nolint:gomnd
}

func (u *Uploader) maybeIgnoreCachedEntry(ctx context.Context, ent fs.Entry) fs.Entry {
	if h, ok := ent.(object.HasObjectID); ok {
		if objectIDPercent(h.ObjectID()) < u.ForceHashPercentage {
			log(ctx).Debugf("ignoring valid cached object: %v", h.ObjectID())
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

func (u *Uploader) processNonDirectories(ctx context.Context, output chan dirEntryOrError, dirRelativePath string, entries fs.Entries, policyTree *policy.Tree, prevEntries []fs.Entries) error {
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
			u.Progress.CachedFile(filepath.Join(dirRelativePath, entry.Name()), entry.Size())

			// compute entryResult now, cachedEntry is short-lived
			cachedDirEntry, err := newDirEntry(entry, cachedEntry.(object.HasObjectID).ObjectID())
			if err != nil {
				return errors.Wrap(err, "unable to create dir entry")
			}

			output <- dirEntryOrError{de: cachedDirEntry}
			return nil
		}

		switch entry := entry.(type) {
		case fs.Symlink:
			de, err := u.uploadSymlinkInternal(ctx, entryRelativePath, entry)
			if err != nil {
				return u.maybeIgnoreFileReadError(err, output, entryRelativePath, policyTree)
			}

			output <- dirEntryOrError{de: de}
			return nil

		case fs.File:
			atomic.AddInt32(&u.stats.NonCachedFiles, 1)
			de, err := u.uploadFileInternal(ctx, entryRelativePath, entry, policyTree.Child(entry.Name()).EffectivePolicy(), asyncWritesPerFile)
			if err != nil {
				return u.maybeIgnoreFileReadError(err, output, entryRelativePath, policyTree)
			}

			output <- dirEntryOrError{de: de}
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

	return skipCacheDirectory(ent)
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
	dirRelativePath string,
) (object.ID, fs.DirectorySummary, error) {
	u.stats.TotalDirectoryCount++

	u.Progress.StartedDirectory(dirRelativePath)
	defer u.Progress.FinishedDirectory(dirRelativePath)

	dirManifest := &snapshot.DirManifest{
		StreamType: directoryStreamType,
		Summary: &fs.DirectorySummary{
			TotalDirCount: 1,
		},
	}

	t0 := u.repo.Time()
	entries, direrr := directory.Readdir(ctx)
	log(ctx).Debugf("finished reading directory %v in %v", dirRelativePath, u.repo.Time().Sub(t0))

	if direrr != nil {
		return "", fs.DirectorySummary{}, dirReadError{direrr}
	}

	entries = skipCacheDirectory(entries)

	var prevEntries []fs.Entries

	for _, d := range uniqueDirectories(previousDirs) {
		if ent := maybeReadDirectoryEntries(ctx, d); ent != nil {
			prevEntries = append(prevEntries, ent)
		}
	}

	if err := u.processChildren(ctx, dirManifest, dirRelativePath, entries, policyTree, prevEntries); err != nil && !errors.Is(err, errCanceled) {
		return "", fs.DirectorySummary{}, err
	}

	if len(dirManifest.Entries) == 0 {
		dirManifest.Summary.MaxModTime = directory.ModTime()
	}

	// at this point dirManifest is ready to go

	writer := u.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "DIR:" + dirRelativePath,
		Prefix:      "k",
	})

	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(dirManifest); err != nil {
		return "", fs.DirectorySummary{}, errors.Wrap(err, "unable to encode directory JSON")
	}

	oid, err := writer.Result()

	dirManifest.Summary.IncompleteReason = u.incompleteReason()

	return oid, *dirManifest.Summary, err
}

func skipCacheDirectory(entries fs.Entries) fs.Entries {
	if entries.FindByName(repo.CacheDirMarkerFile) != nil {
		// if the given directory contains a marker file used for kopia cache, pretend the directory was empty.
		return nil
	}

	return entries
}

func (u *Uploader) maybeIgnoreFileReadError(err error, output chan dirEntryOrError, entryRelativePath string, policyTree *policy.Tree) error {
	errHandlingPolicy := policyTree.EffectivePolicy().ErrorHandlingPolicy

	if u.IgnoreReadErrors || errHandlingPolicy.IgnoreFileErrorsOrDefault(false) {
		err = rootCauseError(err)
		u.Progress.IgnoredError(entryRelativePath, err)
		output <- dirEntryOrError{failedEntry: &fs.EntryWithError{
			EntryPath: entryRelativePath,
			Error:     err.Error(),
		}}

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
		CheckpointInterval: DefaultCheckpointInterval,
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

	maxPreviousTotalFileSize := int64(0)
	maxPreviousFileCount := 0

	for _, m := range previousManifests {
		if s := m.Stats.TotalFileSize; s > maxPreviousTotalFileSize {
			maxPreviousTotalFileSize = s
		}

		if s := m.Stats.TotalFileCount; s > maxPreviousFileCount {
			maxPreviousFileCount = s
		}
	}

	u.Progress.UploadStarted(maxPreviousFileCount, maxPreviousTotalFileSize)
	defer u.Progress.UploadFinished()

	u.stats = snapshot.Stats{}
	u.totalWrittenBytes = 0

	var err error

	s.StartTime = u.repo.Time()

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
		s.RootEntry, err = u.uploadDirWithCheckpointing(ctx, entry, policyTree, previousDirs, sourceInfo)

	case fs.File:
		s.RootEntry, err = u.uploadFile(ctx, entry.Name(), entry, policyTree.EffectivePolicy())

	default:
		return nil, errors.Errorf("unsupported source: %v", s.Source)
	}

	if err != nil {
		return nil, err
	}

	s.IncompleteReason = u.incompleteReason()
	s.EndTime = u.repo.Time()
	s.Stats = u.stats

	return s, nil
}
