package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/dir"
	"github.com/kopia/kopia/internal/hashcache"
	"github.com/kopia/kopia/repo"
)

func hashEntryMetadata(w io.Writer, e *fs.EntryMetadata) {
	binary.Write(w, binary.LittleEndian, e.Name)
	binary.Write(w, binary.LittleEndian, e.ModTime.UnixNano())
	binary.Write(w, binary.LittleEndian, e.FileMode())
	binary.Write(w, binary.LittleEndian, e.FileSize)
	binary.Write(w, binary.LittleEndian, e.UserID)
	binary.Write(w, binary.LittleEndian, e.GroupID)
}

func metadataHash(e *fs.EntryMetadata) uint64 {
	h := fnv.New64a()
	hashEntryMetadata(h, e)
	return h.Sum64()
}

var errCancelled = errors.New("cancelled")

// Uploader supports efficient uploading files and directories to repository.
type Uploader struct {
	Progress       UploadProgress
	Files          FilesPolicy
	MaxUploadBytes int64

	uploadBuf   []byte
	repo        *repo.Repository
	cacheWriter hashcache.Writer
	cacheReader hashcache.Reader

	stats     Stats
	cancelled int32
}

func (u *Uploader) IsCancelled() bool {
	return u.cancelReason() != ""
}

func (u *Uploader) cancelReason() string {
	if c := atomic.LoadInt32(&u.cancelled) != 0; c {
		return "cancelled"
	}

	if mub := u.MaxUploadBytes; mub > 0 && u.repo.Stats().WrittenBytes > mub {
		return "limit reached"
	}

	return ""
}

func (u *Uploader) uploadFileInternal(f fs.File, relativePath string, forceStored bool) (*dir.Entry, uint64, error) {
	u.Progress.Started(relativePath, f.Metadata().FileSize)

	file, err := f.Open()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file: %v", err)
	}
	defer file.Close()

	writer := u.repo.NewWriter(repo.WriterOptions{
		Description: "FILE:" + f.Metadata().Name,
	})
	defer writer.Close()

	written, err := u.copyWithProgress(relativePath, writer, file, 0, f.Metadata().FileSize)
	if err != nil {
		u.Progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	e2, err := file.EntryMetadata()
	if err != nil {
		u.Progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	r, err := writer.Result(forceStored)
	if err != nil {
		u.Progress.Finished(relativePath, f.Metadata().FileSize, err)
		return nil, 0, err
	}

	de := newDirEntry(e2, r)
	de.FileSize = written

	u.Progress.Finished(relativePath, f.Metadata().FileSize, nil)

	return de, metadataHash(&de.EntryMetadata), nil
}

func (u *Uploader) copyWithProgress(path string, dst io.Writer, src io.Reader, completed int64, length int64) (int64, error) {
	if u.uploadBuf == nil {
		u.uploadBuf = make([]byte, 128*1024) // 128 KB buffer
	}

	var written int64

	for {
		if u.IsCancelled() {
			return 0, errCancelled
		}

		readBytes, readErr := src.Read(u.uploadBuf)
		if readBytes > 0 {
			wroteBytes, writeErr := dst.Write(u.uploadBuf[0:readBytes])
			if wroteBytes > 0 {
				written += int64(wroteBytes)
				completed += int64(wroteBytes)
				if length < completed {
					length = completed
				}
				u.Progress.Progress(path, completed, length)
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

func newDirEntry(md *fs.EntryMetadata, oid repo.ObjectID) *dir.Entry {
	return &dir.Entry{
		EntryMetadata: *md,
		ObjectID:      oid,
	}
}

// uploadFile uploads the specified File to the repository.
func (u *Uploader) uploadFile(file fs.File) (repo.ObjectID, error) {
	e, _, err := u.uploadFileInternal(file, file.Metadata().Name, true)
	if err != nil {
		return repo.NullObjectID, err
	}
	return e.ObjectID, nil
}

// uploadDir uploads the specified Directory to the repository.
// An optional ID of a hash-cache object may be provided, in which case the Uploader will use its
// contents to avoid hashing
func (u *Uploader) uploadDir(dir fs.Directory) (repo.ObjectID, repo.ObjectID, error) {
	var err error

	if err := u.repo.BeginPacking(); err != nil {
		return repo.NullObjectID, repo.NullObjectID, err
	}

	mw := u.repo.NewWriter(repo.WriterOptions{
		Description:     "HASHCACHE:" + dir.Metadata().Name,
		BlockNamePrefix: "H",
		PackGroup:       "HC",
	})
	defer mw.Close()
	u.cacheWriter = hashcache.NewWriter(mw)
	oid, err := uploadDirInternal(u, dir, ".", true)
	if u.IsCancelled() {
		if err := u.cacheReader.CopyTo(u.cacheWriter); err != nil {
			return repo.NullObjectID, repo.NullObjectID, err
		}
	}
	u.cacheWriter.Finalize()
	u.cacheWriter = nil

	if err != nil {
		return repo.NullObjectID, repo.NullObjectID, err
	}

	hcid, err := mw.Result(true)
	if err := u.repo.FinishPacking(); err != nil {
		return repo.NullObjectID, repo.NullObjectID, fmt.Errorf("can't finish packing: %v", err)
	}
	return oid, hcid, err
}

func uploadDirInternal(
	u *Uploader,
	directory fs.Directory,
	relativePath string,
	forceStored bool,
) (repo.ObjectID, error) {
	u.Progress.StartedDir(relativePath)
	defer u.Progress.FinishedDir(relativePath)

	u.stats.TotalDirectoryCount++

	entries, err := directory.Readdir()
	if err != nil {
		return repo.NullObjectID, err
	}

	writer := u.repo.NewWriter(repo.WriterOptions{
		Description: "DIR:" + relativePath,
		PackGroup:   "DIR",
	})

	dw := dir.NewWriter(writer)
	defer writer.Close()

	for _, entry := range entries {
		if u.IsCancelled() {
			break
		}
		e := entry.Metadata()
		entryRelativePath := relativePath + "/" + e.Name

		var de *dir.Entry

		var hash uint64

		switch entry := entry.(type) {
		case fs.Directory:
			oid, err := uploadDirInternal(u, entry, entryRelativePath, false)
			if err != nil {
				return repo.NullObjectID, err
			}
			de = newDirEntry(e, oid)

		case fs.Symlink:
			l, err := entry.Readlink()
			if err != nil {
				return repo.NullObjectID, err
			}

			de = newDirEntry(e, repo.InlineObjectID([]byte(l)))
			hash = metadataHash(e)

		case fs.File:
			// regular file
			// See if we had this name during previous pass.
			cachedEntry := u.cacheReader.FindEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			computedHash := metadataHash(e)
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == computedHash

			if cacheMatches {
				u.stats.CachedFiles++
				u.Progress.Cached(entryRelativePath, entry.Metadata().FileSize)
				// Avoid hashing by reusing previous object ID.
				de = newDirEntry(e, cachedEntry.ObjectID)
				hash = cachedEntry.Hash
			} else {
				u.stats.NonCachedFiles++
				de, hash, err = u.uploadFileInternal(entry, entryRelativePath, false)
				if err == errCancelled {
					continue
				}

				if err != nil {
					return repo.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
				}
			}

			u.stats.TotalFileCount++
			u.stats.TotalFileSize += de.FileSize

		default:
			return repo.NullObjectID, fmt.Errorf("file type %v not supported", entry.Metadata().Type)
		}

		if err := dw.WriteEntry(de); err != nil {
			return repo.NullObjectID, err
		}

		if de.Type != fs.EntryTypeDirectory && de.ObjectID.StorageBlock != "" {
			if err := u.cacheWriter.WriteEntry(hashcache.Entry{
				Name:     entryRelativePath,
				Hash:     hash,
				ObjectID: de.ObjectID,
			}); err != nil {
				return repo.NullObjectID, err
			}
		}
	}

	dw.Finalize()

	return writer.Result(forceStored)
}

// NewUploader creates new Uploader object for a given repository.
func NewUploader(r *repo.Repository) *Uploader {
	return &Uploader{
		repo:     r,
		Progress: &nullUploadProgress{},
	}
}

// Cancel requests cancellation of an upload that's in progress. Will typically result in an incomplete snapshot.
func (u *Uploader) Cancel() {
	atomic.StoreInt32(&u.cancelled, 1)
}

// Upload uploads contents of the specified filesystem entry (file or directory) to the repository and returns snapshot.Manifest with statistics.
// Old snapshot manifest, when provided can be used to speed up uploads by utilizing hash cache.
func (u *Uploader) Upload(
	source fs.Entry,
	sourceInfo *SourceInfo,
	old *Manifest,
) (*Manifest, error) {
	s := &Manifest{
		Source: *sourceInfo,
	}

	u.cacheReader = hashcache.Open(nil)
	u.stats = Stats{}
	if old != nil {
		if r, err := u.repo.Open(old.HashCacheID); err == nil {
			u.cacheReader = hashcache.Open(r)
		}
	}

	var err error

	s.StartTime = time.Now()

	switch entry := source.(type) {
	case fs.Directory:
		s.RootObjectID, s.HashCacheID, err = u.uploadDir(entry)

	case fs.File:
		s.RootObjectID, err = u.uploadFile(entry)

	default:
		return nil, fmt.Errorf("unsupported source: %v", s.Source)
	}
	if err != nil {
		return nil, err
	}

	s.IncompleteReason = u.cancelReason()
	s.EndTime = time.Now()
	s.Stats = u.stats
	s.Stats.Repository = u.repo.Status().Stats

	return s, nil
}
