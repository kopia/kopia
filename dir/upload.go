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
	oid, _, err := u.uploadDirInternal(path, previous, 0)

	return oid, err
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

func (u uploader) uploadDirInternal(path string, previous content.ObjectID, previousCRC32 uint32) (content.ObjectID, uint32, error) {
	listing, err := u.lister.List(path)
	if err != nil {
		return content.NullObjectID, 0, err
	}

	var previousListing Listing

	if previous != "" {
		d, err := u.mgr.Open(previous)
		if err == nil {
			previousListing, _ = ReadListing(d)
		}
	}

	h := crc32.NewIEEE()

	// Process all directories first, in canonical order before any files.
	for _, d := range listing.Directories {
		p := previousListing.FindDirectoryByName(d.Name)
		var prevObjectID content.ObjectID
		var prevCRC uint32

		if p != nil {
			prevCRC = p.MetadataCRC32
			prevObjectID = p.ObjectID
		}

		objectID, metadataChecksum, err := u.uploadDirInternal(
			filepath.Join(path, d.Name),
			prevObjectID,
			prevCRC,
		)
		if err != nil {
			return content.NullObjectID, 0, err
		}

		d.ObjectID = objectID
		h.Write([]byte{0})
		binary.Write(h, binary.LittleEndian, metadataChecksum)
	}

	// Process files, for each file read cached chunk ID from streaming cache.
	for _, f := range listing.Files {
		h.Write([]byte{0})
		binary.Write(h, binary.LittleEndian, f.MetadataCRC32)
	}

	dirMetadataChecksum := h.Sum32()

	if dirMetadataChecksum == previousCRC32 {
		return previous, dirMetadataChecksum, nil
	}

	// Upload all files.
	for _, f := range listing.Files {
		fullPath := filepath.Join(path, f.Name)
		if f.ObjectID == content.NullObjectID {
			f.ObjectID, err = u.UploadFile(fullPath)
			if err != nil {
				return content.NullObjectID, 0, fmt.Errorf("unable to hash file: %s", err)
			}
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
			return "", 0, err
		}
	}
	for _, f := range listing.Files {
		if err := dw.WriteEntry(f); err != nil {
			return "", 0, err
		}
	}

	if directoryObjectID, err := writer.Result(true); err == nil {
		return directoryObjectID, dirMetadataChecksum, nil
	}

	return "", 0, err

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
