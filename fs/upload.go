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
	UploadDir(path string, previousObjectID cas.ObjectID) (cas.ObjectID, error)
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

type readaheadDirectory struct {
	src                 Directory
	unreadEntriesByName map[string]Entry
}

func (ra *readaheadDirectory) FindByName(name string) Entry {
	if e, ok := ra.unreadEntriesByName[name]; ok {
		delete(ra.unreadEntriesByName, name)
		return e
	}

	for ra.src != nil && len(ra.unreadEntriesByName) < maxDirReadAheadCount {
		next, ok := <-ra.src
		if !ok {
			ra.src = nil
			break
		}
		if next.Error == nil {
			if next.Entry.Name() == name {
				return next.Entry
			}
			ra.unreadEntriesByName[next.Entry.Name()] = next.Entry
		}
	}

	return nil
}

func (ra *readaheadDirectory) hasUnreadEntries() bool {
	return len(ra.unreadEntriesByName) > 0
}

func (u *uploader) UploadDir(path string, previous cas.ObjectID) (cas.ObjectID, error) {
	var cached = emptyDirectory

	if previous != "" {
		if r, err := u.mgr.Open(previous); err == nil {
			cached, _ = ReadDirectory(r)
		}
	}

	return u.uploadDirInternal(path, previous, cached)
}

func (u *uploader) uploadDirInternal(path string, previous cas.ObjectID, previousDir Directory) (cas.ObjectID, error) {
	if u.isCancelled() {
		return previous, ErrUploadCancelled
	}

	dir, err := u.lister.List(path)
	if err != nil {
		return cas.NullObjectID, err
	}

	ra := readaheadDirectory{
		src:                 previousDir,
		unreadEntriesByName: map[string]Entry{},
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)

	writeDirectoryHeader(writer)
	defer writer.Close()

	directoryMatchesCache := true
	for de := range dir {
		e := de.Entry
		fullPath := filepath.Join(path, e.Name())

		// See if we had this name during previous pass.
		cachedEntry := ra.FindByName(e.Name())

		// ... and whether file metadata is identical to the previous one.
		cachedMetadataMatches := metadataEquals(e, cachedEntry)

		// If not, directoryMatchesCache becomes false.
		directoryMatchesCache = directoryMatchesCache && cachedMetadataMatches

		var oid cas.ObjectID

		if e.IsDir() {
			var previousSubdirObjectID cas.ObjectID
			if cachedEntry != nil {
				previousSubdirObjectID = cachedEntry.ObjectID()
			}

			oid, err = u.UploadDir(fullPath, previousSubdirObjectID)
			if err != nil {
				return cas.NullObjectID, err
			}

			if cachedEntry != nil && oid != cachedEntry.ObjectID() {
				directoryMatchesCache = false
			}
		} else if cachedMetadataMatches {
			// Avoid hashing by reusing previous object ID.
			oid = cachedEntry.ObjectID()
		} else {
			oid, err = u.UploadFile(fullPath)
			if err != nil {
				return cas.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
			}
		}

		e = &entryWithObjectID{Entry: e, oid: oid}
		writeDirectoryEntry(writer, e)
	}

	if ra.hasUnreadEntries() {
		directoryMatchesCache = false
	}

	if directoryMatchesCache && previous != "" {
		// Avoid hashing directory listingif every entry matched the previous (possibly ignoring ordering).
		return previous, nil
	}

	return writer.Result(true)
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
