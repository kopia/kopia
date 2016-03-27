package dir

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/content"
)

type uploader struct {
	mgr    cas.ObjectManager
	lister Lister
}

func (u uploader) UploadDir(path string, previous content.ObjectID) (content.ObjectID, error) {
	return u.uploadDirInternal(path, previous)
}

func (u uploader) UploadFile(path string) (content.ObjectID, error) {
	oid, _, err := u.uploadFileInternal(path)
	return oid, err
}

func (u uploader) uploadFileInternal(path string) (content.ObjectID, uint32, error) {
	file, err := os.Open(path)
	if err != nil {
		return content.NullObjectID, 0, fmt.Errorf("unable to open file %s: %v", path, err)
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
		return content.NullObjectID, 0, err
	}

	s, err := file.Stat()

	return result, computeChecksum(filepath.Base(file.Name()), s.Size(), s.ModTime()), nil
}

func (u uploader) uploadDirInternal(path string, previous content.ObjectID) (content.ObjectID, error) {
	listing, err := u.lister.List(path)
	if err != nil {
		return content.NullObjectID, err
	}

	var previousListing Listing

	if previous != "" {
		d, err := u.mgr.Open(previous)
		if err == nil {
			previousListing, _ = ReadListing(d)
		}
	}

	// Process all directories first, in canonical order before any files.
	for _, d := range listing.Directories {
		p := previousListing.FindDirectoryByName(d.Name)
		var prevObjectID content.ObjectID

		if p != nil {
			prevObjectID = p.ObjectID
		}

		objectID, err := u.uploadDirInternal(
			filepath.Join(path, d.Name),
			prevObjectID,
		)
		if err != nil {
			return content.NullObjectID, err
		}

		d.ObjectID = objectID
	}

	// Upload all files.
	for _, f := range listing.Files {
		fullPath := filepath.Join(path, f.Name)
		f.ObjectID, err = u.UploadFile(fullPath)
		if err != nil {
			return content.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
		}
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)
	defer writer.Close()

	dw := NewWriter(writer)

	for _, d := range listing.Directories {
		if err := dw.WriteEntry(d); err != nil {
			return "", err
		}
	}
	for _, f := range listing.Files {
		if err := dw.WriteEntry(f); err != nil {
			return "", err
		}
	}

	return writer.Result(true)
}

func computeChecksum(fileName string, size int64, modTime time.Time) uint32 {
	hash := crc32.NewIEEE()
	binary.Write(hash, binary.LittleEndian, size)
	binary.Write(hash, binary.LittleEndian, modTime.UnixNano())
	binary.Write(hash, binary.LittleEndian, []byte(fileName))
	return hash.Sum32()
}

func entryObjectIDOrEmpty(e *Entry) content.ObjectID {
	if e == nil {
		return content.NullObjectID
	}

	return e.ObjectID
}
