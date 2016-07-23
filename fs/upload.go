package fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync/atomic"

	"github.com/kopia/kopia/repo"
)

const (
	maxDirReadAheadCount = 256
)

// ErrUploadCancelled is returned when the upload gets cancelled.
var ErrUploadCancelled = errors.New("upload cancelled")

// UploadResult stores results of an upload.
type UploadResult struct {
	ObjectID   repo.ObjectID
	ManifestID repo.ObjectID
	Cancelled  bool

	Stats struct {
		CachedDirectories    int
		CachedFiles          int
		NonCachedDirectories int
		NonCachedFiles       int
	}
}

// Uploader supports efficient uploading files and directories to repository.
type Uploader interface {
	UploadDir(dir Directory, previousManifestID repo.ObjectID) (*UploadResult, error)
	UploadFile(file File) (*UploadResult, error)
	Cancel()
}

type uploader struct {
	repo repo.Repository

	cancelled int32
}

func (u *uploader) isCancelled() bool {
	return atomic.LoadInt32(&u.cancelled) != 0
}

func (u *uploader) uploadFileInternal(f File, relativePath string, forceStored bool) (*EntryMetadata, uint64, error) {
	log.Printf("Uploading file %v", relativePath)
	file, err := f.Open()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file: %v", err)
	}
	defer file.Close()

	writer := u.repo.NewWriter(
		repo.WithDescription("FILE:" + f.Metadata().Name),
	)
	defer writer.Close()

	written, err := io.Copy(writer, file)
	if err != nil {
		return nil, 0, err
	}

	e2, err := file.EntryMetadata()
	if err != nil {
		return nil, 0, err
	}

	e2.ObjectID, err = writer.Result(forceStored)
	if err != nil {
		return nil, 0, err
	}

	if written != e2.FileSize {
		// file changed
	}

	return e2, e2.metadataHash(), nil
}

func (u *uploader) UploadFile(file File) (*UploadResult, error) {
	result := &UploadResult{}
	e, _, err := u.uploadFileInternal(file, file.Metadata().Name, true)
	result.ObjectID = e.ObjectID
	return result, err
}

func (u *uploader) UploadDir(dir Directory, previousManifestID repo.ObjectID) (*UploadResult, error) {
	var mr hashcacheReader
	var err error

	if previousManifestID != "" {
		if r, err := u.repo.Open(previousManifestID); err == nil {
			mr.open(r)
		}
	}

	mw := u.repo.NewWriter(
		repo.WithDescription("HASHCACHE:"+dir.Metadata().Name),
		repo.WithBlockNamePrefix("H"),
	)
	defer mw.Close()
	hcw := newHashCacheWriter(mw)

	result := &UploadResult{}
	result.ObjectID, _, _, err = u.uploadDirInternal(result, dir, ".", hcw, &mr, true)
	if err != nil {
		return result, err
	}

	result.ManifestID, err = mw.Result(true)
	return result, nil
}

func (u *uploader) uploadDirInternal(
	result *UploadResult,
	dir Directory,
	relativePath string,
	hcw *hashcacheWriter,
	mr *hashcacheReader,
	forceStored bool,
) (repo.ObjectID, uint64, bool, error) {
	log.Printf("Uploading dir %v", relativePath)
	defer log.Printf("Finished uploading dir %v", relativePath)

	entries, err := dir.Readdir()
	if err != nil {
		return "", 0, false, err
	}

	writer := u.repo.NewWriter(
		repo.WithDescription("DIR:" + relativePath),
	)

	dw := newDirectoryWriter(writer)
	defer writer.Close()

	allCached := true

	dirHasher := fnv.New64a()
	dirHasher.Write([]byte(relativePath))
	dirHasher.Write([]byte{0})

	for _, entry := range entries {
		e := entry.Metadata()
		entryRelativePath := relativePath + "/" + e.Name

		var hash uint64

		switch entry := entry.(type) {
		case Directory:
			oid, h, wasCached, err := u.uploadDirInternal(result, entry, entryRelativePath, hcw, mr, false)
			if err != nil {
				return "", 0, false, err
			}
			//log.Printf("dirHash: %v %v", fullPath, h)
			hash = h
			allCached = allCached && wasCached
			e.ObjectID = oid

		case Symlink:
			l, err := entry.Readlink()
			if err != nil {
				return "", 0, false, err
			}

			e.ObjectID = repo.NewInlineObjectID([]byte(l))
			hash = e.metadataHash()

		case File:
			// regular file
			// See if we had this name during previous pass.
			cachedEntry := mr.findEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			cacheMatches := (cachedEntry != nil) && cachedEntry.Hash == e.metadataHash()

			allCached = allCached && cacheMatches

			if cacheMatches {
				result.Stats.CachedFiles++
				// Avoid hashing by reusing previous object ID.
				e.ObjectID = repo.ObjectID(cachedEntry.ObjectID)
				hash = cachedEntry.Hash
			} else {
				result.Stats.NonCachedFiles++
				e, hash, err = u.uploadFileInternal(entry, entryRelativePath, false)
				if err != nil {
					return "", 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

		default:
			return "", 0, false, fmt.Errorf("file type %v not supported", entry.Metadata().FileMode)
		}

		if hash != 0 {
			dirHasher.Write([]byte(e.Name))
			dirHasher.Write([]byte{0})
			binary.Write(dirHasher, binary.LittleEndian, hash)
		}

		if err := dw.WriteEntry(e); err != nil {
			return "", 0, false, err
		}

		if !e.FileMode.IsDir() && e.ObjectID.Type().IsStored() {
			if err := hcw.WriteEntry(hashCacheEntry{
				Name:     entryRelativePath,
				Hash:     e.metadataHash(),
				ObjectID: e.ObjectID,
			}); err != nil {
				return "", 0, false, err
			}
		}
	}

	dw.Close()

	var directoryOID repo.ObjectID
	dirHash := dirHasher.Sum64()

	cachedDirEntry := mr.findEntry(relativePath + "/")
	allCached = allCached && cachedDirEntry != nil && cachedDirEntry.Hash == dirHash

	if allCached {
		// Avoid hashing directory listing if every entry matched the cache.
		result.Stats.CachedDirectories++
		directoryOID = repo.ObjectID(cachedDirEntry.ObjectID)
	} else {
		result.Stats.NonCachedDirectories++
		directoryOID, err = writer.Result(forceStored)
		if err != nil {
			return directoryOID, 0, false, err
		}
	}

	if err := hcw.WriteEntry(hashCacheEntry{
		Name:     relativePath + "/",
		ObjectID: directoryOID,
		Hash:     dirHash,
	}); err != nil {
		return "", 0, false, err
	}

	return directoryOID, dirHash, allCached, nil
}

func (u *uploader) Cancel() {
	atomic.StoreInt32(&u.cancelled, 1)
}

// NewUploader creates new Uploader object for the specified Repository
func NewUploader(repo repo.Repository) Uploader {
	return &uploader{
		repo: repo,
	}
}
