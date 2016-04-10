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

// Uploader supports efficient uploading files and directories to CAS.
type Uploader interface {
	UploadFile(path string) (cas.ObjectID, error)
	UploadDir(path string, previousObjectID cas.ObjectID) (cas.ObjectID, cas.ObjectID, error)
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

func (u *uploader) UploadDir(path string, previous cas.ObjectID) (cas.ObjectID, cas.ObjectID, error) {
	var cached Directory

	if previous != "" {
		if r, err := u.mgr.Open(previous); err == nil {
			cached, _ = ReadDirectory(r, "")
		}
	}

	manifestWriter := u.mgr.NewWriter(
		cas.WithDescription("HASHCACHE:"+path),
		cas.WithBlockNamePrefix("H"),
	)
	dw := newDirectoryWriter(manifestWriter)
	oid, err := u.uploadDirInternal(path, dw, previous, cached)
	if err != nil {
		dw.Close()
		return oid, cas.NullObjectID, err
	}

	err = dw.Close()
	if err != nil {
		return oid, cas.NullObjectID, err
	}

	manifestOid, err := manifestWriter.Result(true)
	return oid, manifestOid, nil
}

func (u *uploader) uploadDirInternal(path string, manifest *directoryWriter, previous cas.ObjectID, previousDir Directory) (cas.ObjectID, error) {
	if u.isCancelled() {
		return previous, ErrUploadCancelled
	}

	dir, err := u.lister.List(path)
	if err != nil {
		return cas.NullObjectID, err
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)

	dw := newDirectoryWriter(writer)
	defer writer.Close()

	directoryMatchesCache := len(previousDir) == len(dir)

	for _, e := range dir {
		fullPath := filepath.Join(path, e.Name)

		// See if we had this name during previous pass.
		cachedEntry := previousDir.FindByName(e.IsDir(), e.Name)

		// ... and whether file metadata is identical to the previous one.
		cachedMetadataMatches := metadataEquals(e, cachedEntry)

		// If not, directoryMatchesCache becomes false.
		directoryMatchesCache = directoryMatchesCache && cachedMetadataMatches

		var oid cas.ObjectID

		if e.IsDir() {
			var previousSubdirObjectID cas.ObjectID
			if cachedEntry != nil {
				previousSubdirObjectID = cachedEntry.ObjectID
			}

			oid, err = u.uploadDirInternal(fullPath, manifest, previousSubdirObjectID, nil)
			if err != nil {
				return cas.NullObjectID, err
			}

			if cachedEntry != nil && oid != cachedEntry.ObjectID {
				directoryMatchesCache = false
			}
		} else if cachedMetadataMatches {
			// Avoid hashing by reusing previous object ID.
			oid = cachedEntry.ObjectID
		} else {
			oid, err = u.UploadFile(fullPath)
			if err != nil {
				return cas.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
			}
		}

		e.ObjectID = oid
		dw.WriteEntry(e)
		manifest.WriteEntry(e)
	}

	var oid cas.ObjectID
	if directoryMatchesCache && previous != "" {
		// Avoid hashing directory listingif every entry matched the previous (possibly ignoring ordering).
		oid, err = previous, nil
	} else {
		oid, err = writer.Result(true)
	}

	return oid, err
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
