package dir

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"
)

var (
	header  = []byte("DIRECTORY:v1\n")
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
	OwnerID  uint32    `json:"uid,omitempty"`
	GroupID  uint32    `json:"gid,omitempty"`
	ObjectID string    `json:"objectID"`
}

func serializeManifestEntry(e *Entry) []byte {
	s := serializedDirectoryEntryV1{
		Name:     e.Name,
		Type:     string(e.Type),
		Mode:     strconv.FormatInt(int64(e.Mode), 8),
		ObjectID: string(e.ObjectID),
		OwnerID:  e.UserID,
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
