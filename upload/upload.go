// Package upload manages uploading snapshots to the repository.
package upload

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/dir"
	"github.com/kopia/kopia/internal/hashcache"
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var log = kopialogging.Logger("kopia/upload")

func hashEntryMetadata(w io.Writer, e *fs.EntryMetadata) {
	io.WriteString(w, e.Name)                                  //nolint:errcheck
	binary.Write(w, binary.LittleEndian, e.ModTime.UnixNano()) //nolint:errcheck
	binary.Write(w, binary.LittleEndian, e.FileMode())         //nolint:errcheck
	binary.Write(w, binary.LittleEndian, e.FileSize)           //nolint:errcheck
	binary.Write(w, binary.LittleEndian, e.UserID)             //nolint:errcheck
	binary.Write(w, binary.LittleEndian, e.GroupID)            //nolint:errcheck
}

func metadataHash(e *fs.EntryMetadata) uint64 {
	h := fnv.New64a()
	hashEntryMetadata(h, e)
	return h.Sum64()
}

var errCancelled = errors.New("cancelled")

// Uploader supports efficient uploading files and directories to repository.
type Uploader struct {
	Progress Progress

	FilesPolicy ignorefs.FilesPolicyGetter

	// automatically cancel the Upload after certain number of bytes
	MaxUploadBytes int64

	// ignore file read errors
	IgnoreFileErrors bool

	// probability with hich hashcache entries will be ignored, must be [0..100]
	// 0=always use hash cache if possible
	// 100=never use hash cache
	ForceHashPercentage int

	// Do not hash-cache files younger than this age.
	// Protects from accidentally caching incorrect hashes of files that are being modified.
	HashCacheMinAge time.Duration

	// Number of files to hash and upload in parallel.
	ParallelUploads int

	repo        *repo.Repository
	cacheWriter hashcache.Writer
	cacheReader hashcache.Reader

	hashCacheCutoff time.Time
	stats           snapshot.Stats
	cancelled       int32

	progressMutex          sync.Mutex
	nextProgressReportTime time.Time
	currentProgressDir     string // current directory for reporting progress
	currentDirNumFiles     int    // number of files in current directory
	currentDirCompleted    int64  // bytes completed in current directory
	currentDirTotalSize    int64  // total # of bytes in current directory
}

// IsCancelled returns true if the upload is cancelled.
func (u *Uploader) IsCancelled() bool {
	return u.cancelReason() != ""
}

func (u *Uploader) cancelReason() string {
	if c := atomic.LoadInt32(&u.cancelled) != 0; c {
		return "cancelled"
	}

	if mub := u.MaxUploadBytes; mub > 0 && u.repo.Blocks.Stats().WrittenBytes > mub {
		return "limit reached"
	}

	return ""
}

func (u *Uploader) uploadFileInternal(ctx context.Context, f fs.File, relativePath string) entryResult {
	file, err := f.Open(ctx)
	if err != nil {
		return entryResult{err: fmt.Errorf("unable to open file: %v", err)}
	}
	defer file.Close() //nolint:errcheck

	writer := u.repo.Objects.NewWriter(ctx, object.WriterOptions{
		Description: "FILE:" + f.Metadata().Name,
	})
	defer writer.Close() //nolint:errcheck

	written, err := u.copyWithProgress(relativePath, writer, file, 0, f.Metadata().FileSize)
	if err != nil {
		return entryResult{err: err}
	}

	e2, err := file.EntryMetadata()
	if err != nil {
		return entryResult{err: err}
	}

	r, err := writer.Result()
	if err != nil {
		return entryResult{err: err}
	}

	de := newDirEntry(e2, r)
	de.FileSize = written

	return entryResult{de: de, hash: metadataHash(&de.EntryMetadata)}
}

func (u *Uploader) uploadSymlinkInternal(ctx context.Context, f fs.Symlink, relativePath string) entryResult {
	target, err := f.Readlink(ctx)
	if err != nil {
		return entryResult{err: fmt.Errorf("unable to read symlink: %v", err)}
	}

	writer := u.repo.Objects.NewWriter(ctx, object.WriterOptions{
		Description: "SYMLINK:" + f.Metadata().Name,
	})
	defer writer.Close() //nolint:errcheck

	written, err := u.copyWithProgress(relativePath, writer, bytes.NewBufferString(target), 0, f.Metadata().FileSize)
	if err != nil {
		return entryResult{err: err}
	}

	r, err := writer.Result()
	if err != nil {
		return entryResult{err: err}
	}

	de := newDirEntry(f.Metadata(), r)
	de.FileSize = written
	return entryResult{de: de, hash: metadataHash(&de.EntryMetadata)}
}

func (u *Uploader) addDirProgress(length int64) {
	u.progressMutex.Lock()
	u.currentDirCompleted += length
	c := u.currentDirCompleted
	shouldReport := false
	if time.Now().After(u.nextProgressReportTime) {
		shouldReport = true
		u.nextProgressReportTime = time.Now().Add(100 * time.Millisecond)
	}
	if c == u.currentDirTotalSize {
		shouldReport = true
	}
	u.progressMutex.Unlock()

	if shouldReport {
		u.Progress.Progress(u.currentProgressDir, u.currentDirNumFiles, c, u.currentDirTotalSize, &u.stats)
	}
}

func (u *Uploader) copyWithProgress(path string, dst io.Writer, src io.Reader, completed int64, length int64) (int64, error) {
	uploadBuf := make([]byte, 128*1024) // 128 KB buffer

	var written int64

	for {
		if u.IsCancelled() {
			return 0, errCancelled
		}

		readBytes, readErr := src.Read(uploadBuf)
		if readBytes > 0 {
			wroteBytes, writeErr := dst.Write(uploadBuf[0:readBytes])
			if wroteBytes > 0 {
				written += int64(wroteBytes)
				completed += int64(wroteBytes)
				u.addDirProgress(int64(wroteBytes))
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

func newDirEntry(md *fs.EntryMetadata, oid object.ID) *dir.Entry {
	return &dir.Entry{
		EntryMetadata: *md,
		ObjectID:      oid,
	}
}

// uploadFile uploads the specified File to the repository.
func (u *Uploader) uploadFile(ctx context.Context, file fs.File) (*dir.Entry, error) {
	res := u.uploadFileInternal(ctx, file, file.Metadata().Name)
	if res.err != nil {
		return nil, res.err
	}
	return &dir.Entry{
		EntryMetadata: *file.Metadata(),
		ObjectID:      res.de.ObjectID,
		DirSummary: &fs.DirectorySummary{
			TotalFileCount: 1,
			TotalFileSize:  res.de.FileSize,
			MaxModTime:     res.de.ModTime,
		},
	}, nil
}

// uploadDir uploads the specified Directory to the repository.
// An optional ID of a hash-cache object may be provided, in which case the Uploader will use its
// contents to avoid hashing
func (u *Uploader) uploadDir(ctx context.Context, rootDir fs.Directory) (*dir.Entry, object.ID, error) {
	mw := u.repo.Objects.NewWriter(ctx, object.WriterOptions{
		Description: "HASHCACHE:" + rootDir.Metadata().Name,
		Prefix:      "h",
	})
	defer mw.Close() //nolint:errcheck
	u.cacheWriter = hashcache.NewWriter(mw)
	oid, summ, err := uploadDirInternal(ctx, u, rootDir, ".")
	if u.IsCancelled() {
		if err2 := u.cacheReader.CopyTo(u.cacheWriter); err != nil {
			return nil, "", err2
		}
	}
	defer u.cacheWriter.Finalize() //nolint:errcheck
	u.cacheWriter = nil

	if err != nil {
		return nil, "", err
	}

	hcid, err := mw.Result()
	if flushErr := u.repo.Objects.Flush(ctx); flushErr != nil {
		return nil, "", fmt.Errorf("can't flush pending objects: %v", flushErr)
	}
	return &dir.Entry{
		EntryMetadata: *rootDir.Metadata(),
		ObjectID:      oid,
		DirSummary:    &summ,
	}, hcid, err
}

func (u *Uploader) foreachEntryUnlessCancelled(relativePath string, entries fs.Entries, cb func(entry fs.Entry, entryRelativePath string) error) error {
	for _, entry := range entries {
		if u.IsCancelled() {
			return errCancelled
		}

		e := entry.Metadata()
		entryRelativePath := relativePath + "/" + e.Name

		if err := cb(entry, entryRelativePath); err != nil {
			return err
		}
	}

	return nil
}

type entryResult struct {
	err  error
	de   *dir.Entry
	hash uint64
}

func (u *Uploader) processSubdirectories(ctx context.Context, relativePath string, entries fs.Entries, dw *dir.Writer, summ *fs.DirectorySummary) error {
	return u.foreachEntryUnlessCancelled(relativePath, entries, func(entry fs.Entry, entryRelativePath string) error {
		dir, ok := entry.(fs.Directory)
		if !ok {
			// skip non-directories
			return nil
		}

		e := dir.Metadata()
		oid, subdirsumm, err := uploadDirInternal(ctx, u, dir, entryRelativePath)
		if err == errCancelled {
			return err
		}

		summ.TotalFileCount += subdirsumm.TotalFileCount
		summ.TotalFileSize += subdirsumm.TotalFileSize
		summ.TotalDirCount += subdirsumm.TotalDirCount
		if subdirsumm.MaxModTime.After(summ.MaxModTime) {
			summ.MaxModTime = subdirsumm.MaxModTime
		}

		if err != nil {
			return fmt.Errorf("unable to process directory %q: %s", e.Name, err)
		}

		de := newDirEntry(e, oid)
		de.DirSummary = &subdirsumm
		if err := dw.WriteEntry(de); err != nil {
			return fmt.Errorf("unable to write dir entry: %v", err)
		}

		return nil
	})
}

func (u *Uploader) prepareProgress(relativePath string, entries fs.Entries) {
	u.currentProgressDir = relativePath
	u.currentDirTotalSize = 0
	u.currentDirCompleted = 0
	u.currentDirNumFiles = 0

	// Phase #2 - compute the total size of files in current directory
	_ = u.foreachEntryUnlessCancelled(relativePath, entries, func(entry fs.Entry, entryRelativePath string) error {
		if _, ok := entry.(fs.File); !ok {
			// skip directories
			return nil
		}

		u.currentDirNumFiles++
		u.currentDirTotalSize += entry.Metadata().FileSize
		return nil
	})
}

type uploadWorkItem struct {
	entry             fs.Entry
	entryRelativePath string
	uploadFunc        func() entryResult
	resultChan        chan entryResult
}

func (u *Uploader) prepareWorkItems(ctx context.Context, dirRelativePath string, entries fs.Entries, summ *fs.DirectorySummary) ([]*uploadWorkItem, error) {
	var result []*uploadWorkItem

	resultErr := u.foreachEntryUnlessCancelled(dirRelativePath, entries, func(entry fs.Entry, entryRelativePath string) error {
		if _, ok := entry.(fs.Directory); ok {
			// skip directories
			return nil
		}

		e := entry.Metadata()

		// regular file
		// See if we had this name during previous pass.
		cachedEntry := u.maybeIgnoreHashCacheEntry(u.cacheReader.FindEntry(entryRelativePath))
		var cachedHash uint64
		if cachedEntry != nil {
			cachedHash = cachedEntry.Hash
		}

		// ... and whether file metadata is identical to the previous one.
		computedHash := metadataHash(e)

		switch entry.(type) {
		case fs.File:
			u.stats.TotalFileCount++
			u.stats.TotalFileSize += e.FileSize
			summ.TotalFileCount++
			summ.TotalFileSize += e.FileSize
			if e.ModTime.After(summ.MaxModTime) {
				summ.MaxModTime = e.ModTime
			}
		}

		if cachedHash == computedHash {
			u.stats.CachedFiles++
			u.addDirProgress(e.FileSize)

			// compute entryResult now, cachedEntry is short-lived
			cachedResult := entryResult{
				de:   newDirEntry(e, cachedEntry.ObjectID),
				hash: cachedEntry.Hash,
			}

			// Avoid hashing by reusing previous object ID.
			result = append(result, &uploadWorkItem{
				entry:             entry,
				entryRelativePath: entryRelativePath,
				uploadFunc: func() entryResult {
					return cachedResult
				},
			})
		} else {
			log.Debugf("hash cache miss for %v (cached %v computed %v)", entryRelativePath, cachedHash, computedHash)

			switch entry := entry.(type) {
			case fs.Symlink:
				result = append(result, &uploadWorkItem{
					entry:             entry,
					entryRelativePath: entryRelativePath,
					uploadFunc: func() entryResult {
						return u.uploadSymlinkInternal(ctx, entry, entryRelativePath)
					},
				})

			case fs.File:
				u.stats.NonCachedFiles++
				result = append(result, &uploadWorkItem{
					entry:             entry,
					entryRelativePath: entryRelativePath,
					uploadFunc: func() entryResult {
						return u.uploadFileInternal(ctx, entry, entryRelativePath)
					},
				})

			default:
				return fmt.Errorf("file type %v not supported", entry.Metadata().Type)
			}
		}
		return nil
	})

	return result, resultErr
}

func toChannel(items []*uploadWorkItem) <-chan *uploadWorkItem {
	ch := make(chan *uploadWorkItem)
	go func() {
		defer close(ch)

		for _, wi := range items {
			ch <- wi
		}
	}()

	return ch
}

func (u *Uploader) launchWorkItems(workItems []*uploadWorkItem, wg *sync.WaitGroup) {
	// allocate result channel for each work item.
	for _, it := range workItems {
		it.resultChan = make(chan entryResult, 1)
	}

	workerCount := u.ParallelUploads
	if workerCount == 0 {
		workerCount = runtime.NumCPU()
	}

	ch := toChannel(workItems)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for it := range ch {
				it.resultChan <- it.uploadFunc()
			}
		}()
	}
}

func (u *Uploader) processUploadWorkItems(workItems []*uploadWorkItem, dw *dir.Writer) error {
	var wg sync.WaitGroup
	u.launchWorkItems(workItems, &wg)

	// Read result channels in order.
	for _, it := range workItems {
		result := <-it.resultChan

		if result.err == errCancelled {
			return errCancelled
		}

		if result.err != nil {
			if u.IgnoreFileErrors {
				u.stats.ReadErrors++
				log.Warningf("unable to hash file %q: %s, ignoring", it.entryRelativePath, result.err)
				continue
			}
			return fmt.Errorf("unable to process %q: %s", it.entryRelativePath, result.err)
		}

		if err := dw.WriteEntry(result.de); err != nil {
			return fmt.Errorf("unable to write directory entry: %v", err)
		}

		if result.hash != 0 && it.entry.Metadata().ModTime.Before(u.hashCacheCutoff) {
			if err := u.cacheWriter.WriteEntry(hashcache.Entry{
				Name:     it.entryRelativePath,
				Hash:     result.hash,
				ObjectID: result.de.ObjectID,
			}); err != nil {
				return fmt.Errorf("unable to write hash cache entry: %v", err)
			}
		}
	}

	// wait for workers, this is technically not needed, but let's make sure we don't leak goroutines
	wg.Wait()

	return nil
}

func uploadDirInternal(
	ctx context.Context,
	u *Uploader,
	directory fs.Directory,
	dirRelativePath string,
) (object.ID, fs.DirectorySummary, error) {
	u.stats.TotalDirectoryCount++

	var summ fs.DirectorySummary
	summ.TotalDirCount = 1

	defer func() {
		summ.IncompleteReason = u.cancelReason()
	}()

	entries, direrr := directory.Readdir(ctx)
	if direrr != nil {
		return "", fs.DirectorySummary{}, direrr
	}
	if len(entries) == 0 {
		summ.MaxModTime = directory.Metadata().ModTime
	}

	writer := u.repo.Objects.NewWriter(ctx, object.WriterOptions{
		Description: "DIR:" + dirRelativePath,
		Prefix:      "k",
	})

	dw := dir.NewWriter(writer)
	defer writer.Close() //nolint:errcheck

	if err := u.processSubdirectories(ctx, dirRelativePath, entries, dw, &summ); err != nil {
		return "", fs.DirectorySummary{}, err
	}
	u.prepareProgress(dirRelativePath, entries)

	workItems, workItemErr := u.prepareWorkItems(ctx, dirRelativePath, entries, &summ)
	if workItemErr != nil {
		return "", fs.DirectorySummary{}, workItemErr
	}
	if err := u.processUploadWorkItems(workItems, dw); err != nil {
		return "", fs.DirectorySummary{}, err
	}
	if err := dw.Finalize(&summ); err != nil {
		return "", fs.DirectorySummary{}, fmt.Errorf("unable to finalize directory: %v", err)
	}

	oid, err := writer.Result()
	return oid, summ, err
}

func (u *Uploader) maybeIgnoreHashCacheEntry(e *hashcache.Entry) *hashcache.Entry {
	if rand.Intn(100) < u.ForceHashPercentage {
		return nil
	}

	return e
}

// NewUploader creates new Uploader object for a given repository.
func NewUploader(r *repo.Repository) *Uploader {
	return &Uploader{
		repo:             r,
		Progress:         &nullUploadProgress{},
		HashCacheMinAge:  1 * time.Hour,
		IgnoreFileErrors: true,
		ParallelUploads:  1,
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Uploader) Cancel() {
	atomic.StoreInt32(&u.cancelled, 1)
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and returns snapshot.Manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up uploads by utilizing hash cache.
func (u *Uploader) Upload(
	ctx context.Context,
	source fs.Entry,
	sourceInfo snapshot.SourceInfo,
	old *snapshot.Manifest,
) (*snapshot.Manifest, error) {
	s := &snapshot.Manifest{
		Source: sourceInfo,
	}

	defer u.Progress.UploadFinished()

	u.cacheReader = hashcache.Open(nil)
	u.stats = snapshot.Stats{}
	if old != nil {
		log.Debugf("opening hash cache: %v", old.HashCacheID)
		if r, err := u.repo.Objects.Open(ctx, old.HashCacheID); err == nil {
			u.cacheReader = hashcache.Open(r)
			log.Debugf("opened hash cache: %v", old.HashCacheID)
		} else {
			log.Warningf("unable to open hash cache %v: %v", old.HashCacheID, err)

		}
	}

	var err error

	s.StartTime = time.Now()
	u.hashCacheCutoff = time.Now().Add(-u.HashCacheMinAge)
	s.HashCacheCutoffTime = u.hashCacheCutoff

	switch entry := source.(type) {
	case fs.Directory:
		entry = ignorefs.New(entry, u.FilesPolicy, ignorefs.ReportIgnoredFiles(func(_ string, md *fs.EntryMetadata) {
			u.stats.AddExcluded(md)
		}))
		s.RootEntry, s.HashCacheID, err = u.uploadDir(ctx, entry)

	case fs.File:
		s.RootEntry, err = u.uploadFile(ctx, entry)

	default:
		return nil, fmt.Errorf("unsupported source: %v", s.Source)
	}

	if err != nil {
		return nil, err
	}

	s.IncompleteReason = u.cancelReason()
	s.EndTime = time.Now()
	s.Stats = u.stats
	s.Stats.Block = u.repo.Blocks.Stats()

	return s, nil
}
