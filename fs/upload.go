package fs

import (
	"errors"
	"fmt"
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

type UploadResult struct {
	ObjectID   cas.ObjectID
	ManifestID cas.ObjectID

	Stats struct {
		CachedDirectories    int
		CachedFiles          int
		NonCachedDirectories int
		NonCachedFiles       int
	}
}

// Uploader supports efficient uploading files and directories to CAS.
type Uploader interface {
	UploadFile(path string) (cas.ObjectID, error)
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

func (u *uploader) UploadFile(path string) (cas.ObjectID, error) {
	file, err := os.Open(path)
	if err != nil {
		return cas.NullObjectID, fmt.Errorf("unable to open file %s: %v", path, err)
	}
	defer file.Close()

	writer := u.mgr.NewWriter(
		cas.WithDescription("FILE:"+path),
		cas.WithBlockNamePrefix("F"),
	)
	defer writer.Close()

	io.Copy(writer, file)
	result, err := writer.Result(false)
	if err != nil {
		return cas.NullObjectID, err
	}

	return result, nil
}

func (u *uploader) UploadDir(path string, previousManifestID cas.ObjectID) (*UploadResult, error) {
	//log.Printf("UploadDir", path)
	//defer log.Printf("finishing UploadDir", path)
	var hcr hashcacheReader
	var err error

	if previousManifestID != "" {
		if r, err := u.mgr.Open(previousManifestID); err == nil {
			if dr, err := newDirectoryReader(r); err == nil {
				hcr.Open(dr)
			}
		}
	}

	manifestWriter := u.mgr.NewWriter(
		cas.WithDescription("HASHCACHE:"+path),
		cas.WithBlockNamePrefix("H"),
	)
	dw := newDirectoryWriter(manifestWriter)

	result := &UploadResult{}
	result.ObjectID, _, err = u.uploadDirInternal(result, path, ".", dw, &hcr)
	if err != nil {
		dw.Close()
		return result, err
	}

	err = dw.Close()
	if err != nil {
		return result, err
	}

	result.ManifestID, err = manifestWriter.Result(true)
	return result, nil
}

func (u *uploader) uploadDirInternal(
	result *UploadResult,
	path string,
	relativePath string,
	hcw *directoryWriter,
	hcr *hashcacheReader,
) (cas.ObjectID, bool, error) {
	dir, err := u.lister.List(path)
	if err != nil {
		return cas.NullObjectID, false, err
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)

	dw := newDirectoryWriter(writer)
	defer writer.Close()

	allCached := true

	for _, e := range dir {
		fullPath := filepath.Join(path, e.Name)
		entryRelativePath := relativePath + "/" + e.Name

		if e.IsDir() {
			oid, wasCached, err := u.uploadDirInternal(result, fullPath, entryRelativePath, hcw, hcr)
			if err != nil {
				return cas.NullObjectID, false, err
			}

			allCached = allCached && wasCached
			e.ObjectID = oid
		} else {
			// See if we had this name during previous pass.
			cachedEntry, numSkipped := hcr.GetEntry(entryRelativePath)

			// ... and whether file metadata is identical to the previous one.
			cacheMatches := metadataEquals(e, cachedEntry)

			allCached = allCached && cacheMatches && numSkipped == 0

			if cacheMatches {
				result.Stats.CachedFiles++
				// Avoid hashing by reusing previous object ID.
				e.ObjectID = cachedEntry.ObjectID
			} else {
				result.Stats.NonCachedFiles++
				e.ObjectID, err = u.UploadFile(fullPath)
				if err != nil {
					return cas.NullObjectID, false, fmt.Errorf("unable to hash file: %s", err)
				}
			}
		}

		if err := dw.WriteEntry(e); err != nil {
			return cas.NullObjectID, false, err
		}

		if e.IsDir() {
			e.Name = entryRelativePath + "/"
		} else {
			e.Name = entryRelativePath
		}
		if err := hcw.WriteEntry(e); err != nil {
			return cas.NullObjectID, false, err
		}
	}

	var directoryOID cas.ObjectID

	cachedDirEntry, numSkipped := hcr.GetEntry(relativePath + "/")
	allCached = allCached && cachedDirEntry != nil && numSkipped == 0

	if allCached {
		// Avoid hashing directory listing if every entry matched the cache.
		result.Stats.CachedDirectories++
		return cachedDirEntry.ObjectID, true, nil
	} else {
		result.Stats.NonCachedDirectories++
		directoryOID, err = writer.Result(true)
		return directoryOID, false, err
	}
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
