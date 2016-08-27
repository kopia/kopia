package fs

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

// EntryType is a type of a filesystem entry.
type EntryType string

// Supported entry types.
const (
	EntryTypeUnknown   EntryType = ""  // unknown type
	EntryTypeFile      EntryType = "f" // file
	EntryTypeDirectory EntryType = "d" // directory
	EntryTypeSymlink   EntryType = "s" // symbolic link
)

// Permissions encapsulates UNIX permissions for a filesystem entry.
type Permissions int

// MarshalJSON emits permissions as octal string.
func (p Permissions) MarshalJSON() ([]byte, error) {
	if p == 0 {
		return nil, nil
	}

	s := "0" + strconv.FormatInt(int64(p), 8)
	return json.Marshal(&s)
}

// UnmarshalJSON parses octal permissions string from JSON.
func (p *Permissions) UnmarshalJSON(b []byte) error {
	var s string

	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	v, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return err
	}

	*p = Permissions(v)
	return nil
}

// EntryMetadata stores attributes of a single entry in a directory.
type EntryMetadata struct {
	Name        string      `json:"name,omitempty"`
	Type        EntryType   `json:"type,omitempty"`
	Permissions Permissions `json:"mode,omitempty"`
	FileSize    int64       `json:"size,omitempty"`
	ModTime     time.Time   `json:"mtime,omitempty"`
	UserID      uint32      `json:"uid,omitempty"`
	GroupID     uint32      `json:"gid,omitempty"`
}

// FileMode returns os.FileMode corresponding to Type and Permissions of the entry metadata.
func (e *EntryMetadata) FileMode() os.FileMode {
	perm := os.FileMode(e.Permissions)

	switch e.Type {
	default:
		return perm

	case EntryTypeFile:
		return perm

	case EntryTypeDirectory:
		return perm | os.ModeDir

	case EntryTypeSymlink:
		return perm | os.ModeSymlink
	}
}
