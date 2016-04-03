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
	directoryHeader = []byte("DIRECTORY:v1")
	newLine         = []byte("\n")
)

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

func writeDirectoryHeader(w io.Writer) error {
	if _, err := w.Write(directoryHeader); err != nil {
		return err
	}
	if _, err := w.Write(newLine); err != nil {
		return err
	}

	return nil
}

func writeDirectoryEntry(w io.Writer, e *Entry) error {
	if e.Type == "" {
		return errors.New("missing entry type")
	}

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
		var err error

		for s.Scan() {
			line := s.Bytes()
			var v serializedDirectoryEntryV1
			if err := json.Unmarshal(line, &v); err != nil {
				ch <- EntryOrError{Error: err}
			}

			e := &Entry{}
			e.Name = v.Name
			e.UserID = v.UserID
			e.GroupID = v.GroupID
			e.ObjectID, err = cas.ParseObjectID(v.ObjectID)
			if err != nil {
				ch <- EntryOrError{Error: err}
				continue
			}
			m, err := strconv.ParseInt(v.Mode, 8, 16)
			if err != nil {
				ch <- EntryOrError{Error: err}
				continue
			}
			e.Mode = int16(m)
			e.ModTime = v.ModTime
			e.Type = EntryType(v.Type)
			if e.Type == EntryTypeFile {
				if v.FileSize == nil {
					ch <- EntryOrError{Error: fmt.Errorf("missing file size")}
					continue
				}

				e.Size = *v.FileSize
			}

			ch <- EntryOrError{Entry: e}
		}
		close(ch)
	}()

	return ch, nil
}
