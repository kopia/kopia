package fs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/kopia/kopia/cas"
)

var (
	header  = []byte("DIRECTORY:v1")
	newLine = []byte("\n")
)

// Writer allows writing directories.
type Writer interface {
	WriteEntry(e *Entry) error
}

type writer struct {
	lastEntryType EntryType
	objectWriter  io.Writer
}

type serializedDirectoryEntryV1 struct {
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	FileSize *int64    `json:"size,omitempty,string"`
	Mode     string    `json:"mode"`
	ModTime  time.Time `json:"modified,omitempty"`
	UserID   uint32    `json:"uid,omitempty"`
	GroupID  uint32    `json:"gid,omitempty"`
	ObjectID string    `json:"objectID"`
}

func serializeManifestEntry(e *Entry) []byte {
	s := serializedDirectoryEntryV1{
		Name:     e.Name,
		Type:     string(e.Type),
		Mode:     strconv.FormatInt(int64(e.Mode), 8),
		ObjectID: string(e.ObjectID),
		UserID:   e.UserID,
		GroupID:  e.GroupID,
	}

	s.ModTime = e.ModTime.UTC()

	if e.Type == EntryTypeFile {
		fs := e.Size
		s.FileSize = &fs
	}

	jsonBytes, _ := json.Marshal(s)
	return jsonBytes
}

func (w *writer) WriteEntry(e *Entry) error {
	if e.Type == "" {
		return errors.New("missing entry type")
	}

	if w.lastEntryType == "" {
		if _, err := w.objectWriter.Write(header); err != nil {
			return err
		}
		if _, err := w.objectWriter.Write(newLine); err != nil {
			return err
		}
	} else {
		if w.lastEntryType != e.Type && e.Type == EntryTypeDirectory {
			return errors.New("directories must be added before non-directories")
		}
	}
	w.lastEntryType = e.Type

	s := serializeManifestEntry(e)
	if _, err := w.objectWriter.Write(s); err != nil {
		return err
	}
	if _, err := w.objectWriter.Write(newLine); err != nil {
		return err
	}
	return nil
}

// NewWriter creates a Writer object that writes directory contents to the specified underlying writer.
func NewWriter(w io.Writer) Writer {
	return &writer{
		objectWriter: w,
	}
}

func WriteDir(w io.Writer, dir Directory) error {
	dw := NewWriter(w)

	for _, d := range dir.Entries {
		if err := dw.WriteEntry(d); err != nil {
			return err
		}
	}

	return nil
}

func ReadDir(r io.Reader) (Directory, error) {
	var err error

	s := bufio.NewScanner(r)
	if !s.Scan() {
		return Directory{}, fmt.Errorf("empty file")
	}

	if !bytes.Equal(s.Bytes(), header) {
		return Directory{}, fmt.Errorf("invalid header: expected '%v' got '%v'", header, s.Bytes())
	}

	l := Directory{}

	for s.Scan() {
		line := s.Bytes()
		var v serializedDirectoryEntryV1
		if err := json.Unmarshal(line, &v); err != nil {
			return Directory{}, nil
		}

		e := &Entry{}
		e.Name = v.Name
		e.UserID = v.UserID
		e.GroupID = v.GroupID
		e.ObjectID, err = cas.ParseObjectID(v.ObjectID)
		if err != nil {
			return Directory{}, nil
		}
		m, err := strconv.ParseInt(v.Mode, 8, 16)
		if err != nil {
			return Directory{}, nil
		}
		e.Mode = int16(m)
		e.ModTime = v.ModTime
		e.Type = EntryType(v.Type)
		if e.Type == EntryTypeFile {
			if v.FileSize == nil {
				return Directory{}, fmt.Errorf("missing file size")
			}

			e.Size = *v.FileSize
		}

		l.Entries = append(l.Entries, e)
	}
	return l, nil
}
