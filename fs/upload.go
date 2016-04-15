package fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/kopia/kopia/cas"
)

const (
	maxDirReadAheadCount = 256
)

// ErrUploadCancelled is returned when the upload gets cancelled.
var ErrUploadCancelled = errors.New("upload cancelled")

// UploadResult stores results of an upload.
type UploadResult struct {
	ObjectID   cas.ObjectID
	ManifestID cas.ObjectID
	Cancelled  bool

	Stats struct {
		CachedDirectories    int
		CachedFiles          int
		NonCachedDirectories int
		NonCachedFiles       int
	}
}

// Uploader supports efficient uploading files and directories to CAS.
type Uploader interface {
	UploadDir(path string, previousManifestID cas.ObjectID) (*UploadResult, error)
	Cancel()
}

type uploader struct {
	mgr    cas.ObjectManager
	lister Lister

	cancelled int32
}

func (u *uploader) isCancelled() bool {
	return atomic.LoadInt32(&u.cancelled) != 0
}

func (u *uploader) uploadFile(path string, e *Entry) (*Entry, uint64, error) {
	file, e2, err := u.lister.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to open file %s: %v", path, err)
	}
	defer file.Close()

	writer := u.mgr.NewWriter(
		cas.WithDescription("FILE:"+path),
		cas.WithBlockNamePrefix("F"),
	)
	defer writer.Close()

	io.Copy(writer, file)
	e2.ObjectID, err = writer.Result(false)
	if err != nil {
		return nil, 0, err
	}

	return e2, e2.metadataHash(), nil
}

func (u *uploader) UploadDir(path string, previousManifestID cas.ObjectID) (*UploadResult, error) {
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
		cas.WithDescription("HASHCACHE:"+path),
		cas.WithBlockNamePrefix("H"),
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
) (cas.ObjectID, uint64, bool, error) {
	dir, err := u.lister.List(path)
	if err != nil {
		return cas.NullObjectID, 0, false, err
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
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
				return cas.NullObjectID, 0, false, err
			}
			//log.Printf("dirHash: %v %v", fullPath, h)
			hash = h
			allCached = allCached && wasCached
			e.ObjectID = oid

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
				e.ObjectID = cas.ObjectID(cachedEntry.ObjectID)
				hash = cachedEntry.Hash
			} else {
				result.Stats.NonCachedFiles++
				e, hash, err = u.uploadFile(fullPath, e)
				if err != nil {
					return cas.NullObjectID, 0, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}

		default:
			return cas.NullObjectID, 0, false, fmt.Errorf("file type %v not supported", e.FileMode)
		}

		dirHasher.Write([]byte(e.Name))
		dirHasher.Write([]byte{0})
		binary.Write(dirHasher, binary.LittleEndian, hash)

		if err := dw.WriteEntry(e); err != nil {
			return cas.NullObjectID, 0, false, err
		}

		if !e.FileMode.IsDir() {
			if err := hcw.WriteEntry(hashCacheEntry{
				Name:     entryRelativePath,
				Hash:     e.metadataHash(),
				ObjectID: e.ObjectID,
			}); err != nil {
				return cas.NullObjectID, 0, false, err
			}
		}
	}

	var directoryOID cas.ObjectID
	dirHash := dirHasher.Sum64()

	cachedDirEntry := mr.findEntry(relativePath + "/")
	allCached = allCached && cachedDirEntry != nil && cachedDirEntry.Hash == dirHash

	if allCached {
		// Avoid hashing directory listing if every entry matched the cache.
		result.Stats.CachedDirectories++
		directoryOID = cas.ObjectID(cachedDirEntry.ObjectID)
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
		return cas.NullObjectID, 0, false, err
	}

	return directoryOID, dirHash, allCached, nil
}

func (u *uploader) Cancel() {
	atomic.StoreInt32(&u.cancelled, 1)
}

// NewUploader creates new Uploader object for the specified ObjectManager
func NewUploader(mgr cas.ObjectManager) (Uploader, error) {
	u := &uploader{
		mgr:    mgr,
		lister: &filesystemLister{},
	}

	return u, nil
}
