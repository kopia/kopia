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
	mgr       cas.ObjectManager
	lister    Lister
	hashCache hashCache
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

func (u uploader) UploadDir(path string) (content.ObjectID, error) {
	listing, err := u.lister.List(path)
	if err != nil {
		return content.NullObjectID, err
	}

	_ = u.hashCache.GetCachedListing(path)

	// Process all directories first, in canonical order before any files.
	for _, e := range listing.Entries {
		fullPath := filepath.Join(path, e.Name)
		if e.Type == EntryTypeDirectory {
			e.ObjectID, err = u.UploadDir(fullPath)
			if err != nil {
				return content.NullObjectID, err
			}
		} else {
			e.ObjectID, err = u.UploadFile(fullPath)
			if err != nil {
				return content.NullObjectID, fmt.Errorf("unable to hash file: %s", err)
			}
		}
	}

	writer := u.mgr.NewWriter(
		cas.WithDescription("DIR:"+path),
		cas.WithBlockNamePrefix("D"),
	)
	defer writer.Close()

	dw := NewWriter(writer)

	for _, d := range listing.Entries {
		if err := dw.WriteEntry(d); err != nil {
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
