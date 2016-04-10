package fs

import (
	"os"
	"time"

	"github.com/kopia/kopia/cas"
)

// Entry stores attributes of a single entry in a directory.
type Entry struct {
	Name     string
	FileMode os.FileMode
	FileSize int64
	ModTime  time.Time
	UserID   uint32
	GroupID  uint32
	ObjectID cas.ObjectID
}

func (e *Entry) IsDir() bool {
	return e.FileMode.IsDir()
}

func metadataEquals(e1, e2 *Entry) bool {
	if (e1 != nil) != (e2 != nil) {
		return false
	}

	if e1.FileMode != e2.FileMode {
		return false
	}

	if e1.ModTime != e2.ModTime {
		return false
	}

	if e1.FileSize != e2.FileSize {
		return false
	}

	if e1.Name != e2.Name {
		return false
	}

	if e1.UserID != e2.UserID {
		return false
	}

	if e1.GroupID != e2.GroupID {
		return false
	}

	return true
}
