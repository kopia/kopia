package dir

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/content"
)

// Uploader supports efficient uploading files and directories to CAS storage.
type Uploader interface {
	UploadFile(path string) (content.ObjectID, error)
	UploadDir(path string) (content.ObjectID, error)
}

type uploader struct {
	mgr       cas.ObjectManager
	lister    Lister
	hashCache HashCache
}

func (u *uploader) UploadFile(path string) (content.ObjectID, error) {
	file, err := os.Open(path)
	if err != nil {
		return content.NullObjectID, fmt.Errorf("unable to open file %s: %v", path, err)
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
		return content.NullObjectID, err
	}

	return result, nil
}

func (u *uploader) UploadDir(path string) (content.ObjectID, error) {
	listing, err := u.lister.List(path)
	if err != nil {
		return content.NullObjectID, err
	}

	cachedOID := u.hashCache.Get(path)
	var cached Listing

	if cachedOID != "" {
		if r, err := u.mgr.Open(cachedOID); err == nil {
			cached, err = ReadDir(r)
			if err != nil {
				log.Printf("WARNING: unable to cached read directory: %v", err)
			}
		}
	}

	directoryMatchesCache := len(cached.Entries) == len(listing.Entries)
	for _, e := range listing.Entries {
		fullPath := filepath.Join(path, e.Name)

		// See if we had this name during previous pass.
		cachedEntry := cached.FindEntryName(e.Name)

		// ... and whether file metadata is identical to the previous one.
		cachedMetadataMatches := e.metadataEquals(cachedEntry)

		// If not, directoryMatchesCache becomes false.
		directoryMatchesCache = directoryMatchesCache && cachedMetadataMatches

		if e.Type == EntryTypeDirectory {
			e.ObjectID, err = u.UploadDir(fullPath)
			if err != nil {
				return content.NullObjectID, err
			}

			if cachedEntry != nil && e.ObjectID != cachedEntry.ObjectID {
				directoryMatchesCache = false
			}
		} else if cachedMetadataMatches {
			// Avoid hashing by reusing previous object ID.
			e.ObjectID = cachedEntry.ObjectID
		} else {
			e.ObjectID, err = u.UploadFile(fullPath)
			if err != nil {
				return content.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
			}
		}
	}

	if directoryMatchesCache && cachedOID != "" {
		return cachedOID, nil
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)
	defer writer.Close()

	WriteDir(writer, listing)

	oid, err := writer.Result(true)
	if err == nil {
		u.hashCache.Put(path, oid)
	}

	return oid, err
}

// NewUploader creates new Uploader object for the specified ObjectManager
func NewUploader(mgr cas.ObjectManager, hashCache HashCache) (Uploader, error) {
	u := &uploader{
		mgr:       mgr,
		lister:    &filesystemLister{},
		hashCache: hashCache,
	}

	if u.hashCache == nil {
		u.hashCache = &nullHashCache{}
	}

	return u, nil
}
