package fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
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
	UploadDir(path string, previousManifestID repo.ObjectID) (*UploadResult, error)
	Cancel()
}

type uploader struct {
	mgr    repo.Repository
	lister Lister

	cancelled int32
}

func (u *uploader) isCancelled() bool {
	return atomic.LoadInt32(&u.cancelled) != 0
}

func (u *uploader) uploadFile(path string, e *Entry) (*Entry, uint64, error) {
	log.Printf("Uploading file %v", path)
	file, err := u.lister.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file %s: %v", path, err)
	}
	defer file.Close()

	writer := u.mgr.NewWriter(
		repo.WithDescription("FILE:" + path),
	)
	defer writer.Close()

	written, err := io.Copy(writer, file)
	if err != nil {
		return nil, 0, err
	}

	e2, err := file.Entry()
	if err != nil {
		return nil, 0, err
	}

	e2.ObjectID, err = writer.Result(false)
	if err != nil {
		return nil, 0, err
	}

	if written != e2.FileSize {
		// file changed
	}

	return e2, e2.metadataHash(), nil
}

func (u *uploader) UploadDir(path string, previousManifestID repo.ObjectID) (*UploadResult, error) {
	//log.Printf("UploadDir", path)
	//defer log.Printf("finishing UploadDir", path)
	var mr hashcacheReader
	var err error

	if previousManifestID != "" {
		if r, err := u.mgr.Open(previousManifestID); err == nil {
			mr.open(r)
		}
	}

	mw := u.mgr.NewWriter(
		repo.WithDescription("HASHCACHE:"+path),
		repo.WithBlockNamePrefix("H"),
	)
	defer mw.Close()
	hcw := newHashCacheWriter(mw)

	result := &UploadResult{}
	result.ObjectID, _, _, err = u.uploadDirInternal(result, path, ".", hcw, &mr)
	if err != nil {
		return result, err
	}

	result.ManifestID, err = mw.Result(true)
	return result, nil
}

func (u *uploader) uploadDirInternal(
	result *UploadResult,
	path string,
	relativePath string,
	hcw *hashcacheWriter,
	mr *hashcacheReader,
) (repo.ObjectID, uint64, bool, error) {
	log.Printf("Uploading dir %v", path)
	defer log.Printf("Finished uploading dir %v", path)
	dir, err := u.lister.List(path)
	if err != nil {
		return "", 0, false, err
	}

	writer := u.mgr.NewWriter(
		repo.WithDescription("DIR:" + path),
	)

	dw := newDirectoryWriter(writer)
	defer writer.Close()

	allCached := true

	dirHasher := fnv.New64a()
	dirHasher.Write([]byte(relativePath))
	dirHasher.Write([]byte{0})

	for _, e := range dir {
		fullPath := filepath.Join(path, e.Name)
		entryRelativePath := relativePath + "/" + e.Name

		var hash uint64

		switch e.FileMode & os.ModeType {
		case os.ModeDir:
			oid, h, wasCached, err := u.uploadDirInternal(result, fullPath, entryRelativePath, hcw, mr)
			if err != nil {
				return "", 0, false, err
			}
			//log.Printf("dirHash: %v %v", fullPath, h)
			hash = h
			allCached = allCached && wasCached
			e.ObjectID = oid

		case os.ModeSymlink:
			l, err := os.Readlink(fullPath)
			if err != nil {
				return "", 0, false, err
			}

			e.ObjectID = repo.NewInlineObjectID([]byte(l))
			hash = e.metadataHash()

		case 0:
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
				e, hash, err = u.uploadFile(fullPath, e)
				if err != nil {
					return "", 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

		default:
			return "", 0, false, fmt.Errorf("file type %v not supported", e.FileMode)
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
		directoryOID, err = writer.Result(true)
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
func NewUploader(mgr repo.Repository) (Uploader, error) {
	return newUploaderLister(mgr, &filesystemLister{})
}

func newUploaderLister(mgr repo.Repository, lister Lister) (Uploader, error) {
	u := &uploader{
		mgr:    mgr,
		lister: lister,
	}

	return u, nil
}
