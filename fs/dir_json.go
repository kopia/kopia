package fs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/kopia/kopia/cas"
)

var (
	directoryHeader = []byte("DIRECTORY:v1")
	newLine         = []byte("\n")
)

type serializedDirectoryEntryV1 struct {
	XName     string    `json:"name"`
	XFileMode uint32    `json:"mode"`
	XFileSize int64     `json:"size,omitempty,string"`
	XModTime  time.Time `json:"modified,omitempty"`
	XUserID   uint32    `json:"uid,omitempty"`
	XGroupID  uint32    `json:"gid,omitempty"`
	XObjectID string    `json:"oid,omitempty"`
}

func (de *serializedDirectoryEntryV1) Name() string {
	return de.XName
}

func (de *serializedDirectoryEntryV1) Mode() os.FileMode {
	return os.FileMode(de.XFileMode)
}

func (de *serializedDirectoryEntryV1) IsDir() bool {
	return de.Mode().IsDir()
}

func (de *serializedDirectoryEntryV1) Size() int64 {
	if de.Mode().IsRegular() {
		return de.XFileSize
	}

	return 0
}

func (de *serializedDirectoryEntryV1) UserID() uint32 {
	return de.XUserID
}

func (de *serializedDirectoryEntryV1) GroupID() uint32 {
	return de.XGroupID
}

func (de *serializedDirectoryEntryV1) ModTime() time.Time {
	return de.XModTime
}

func (de *serializedDirectoryEntryV1) ObjectID() cas.ObjectID {
	return cas.ObjectID(de.XObjectID)
}

func (de *serializedDirectoryEntryV1) Sys() interface{} {
	return nil
}

func serializeManifestEntry(e Entry) []byte {
	s := serializedDirectoryEntryV1{
		XName:     e.Name(),
		XFileMode: uint32(e.Mode()),
		XObjectID: string(e.ObjectID()),
		XUserID:   e.UserID(),
		XGroupID:  e.GroupID(),
		XModTime:  e.ModTime().UTC(),
	}

	if e.Mode().IsRegular() {
		s.XFileSize = e.Size()
	}

	jsonBytes, _ := json.Marshal(s)
	return jsonBytes
}

func writeDirectoryHeader(w io.Writer) error {
	if _, err := w.Write(directoryHeader); err != nil {
		return err
	}
	if _, err := w.Write(newLine); err != nil {
		return err
	}

	return nil
}

func writeDirectoryEntry(w io.Writer, e Entry) error {
	s := serializeManifestEntry(e)
	if _, err := w.Write(s); err != nil {
		return err
	}
	if _, err := w.Write(newLine); err != nil {
		return err
	}

	return nil
}

func ReadDirectory(r io.Reader) (Directory, error) {
	s := bufio.NewScanner(r)
	if !s.Scan() {
		return nil, fmt.Errorf("empty file")
	}

	if !bytes.Equal(s.Bytes(), directoryHeader) {
		return nil, fmt.Errorf("invalid directoryHeader: expected '%v' got '%v'", directoryHeader, s.Bytes())
	}

	ch := make(Directory)
	go func() {
		for s.Scan() {
			line := s.Bytes()
			var v serializedDirectoryEntryV1
			if err := json.Unmarshal(line, &v); err != nil {
				ch <- EntryOrError{Error: err}
				continue
			}

			ch <- EntryOrError{Entry: &v}
		}
		close(ch)
	}()

	return ch, nil
}
