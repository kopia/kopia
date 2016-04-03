package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/kopia/kopia/cas"
)

// EntryType describes the type of an backup entry.
type EntryType string

const (
	// EntryTypeFile represents a regular file.
	EntryTypeFile EntryType = "f"

	// EntryTypeDirectory represents a directory entry which is a subdirectory.
	EntryTypeDirectory EntryType = "d"

	// EntryTypeSymlink represents a symbolic link.
	EntryTypeSymlink EntryType = "l"

	// EntryTypeSocket represents a UNIX socket.
	EntryTypeSocket EntryType = "s"

	// EntryTypeDevice represents a device.
	EntryTypeDevice EntryType = "v"

	// EntryTypeNamedPipe represents a named pipe.
	EntryTypeNamedPipe EntryType = "n"
)

// FileModeToType converts os.FileMode into EntryType.
func FileModeToType(mode os.FileMode) EntryType {
	switch mode & os.ModeType {
	case os.ModeDir:
		return EntryTypeDirectory

	case os.ModeDevice:
		return EntryTypeDevice

	case os.ModeSocket:
		return EntryTypeSocket

	case os.ModeSymlink:
		return EntryTypeSymlink

	case os.ModeNamedPipe:
		return EntryTypeNamedPipe

	default:
		return EntryTypeFile
	}
}

// EntryMetadata stores metadata about a directory entry but not related its content.
type EntryMetadata struct {
	Name    string
	Size    int64
	Type    EntryType
	ModTime time.Time
	Mode    int16 // 0000 .. 0777
	UserID  uint32
	GroupID uint32
}

// Entry stores attributes of a single entry in a directory.
type Entry struct {
	EntryMetadata

	ObjectID cas.ObjectID
}

func (e *Entry) metadataEquals(other *Entry) bool {
	if other == nil {
		return false
	}

	return e.EntryMetadata == other.EntryMetadata
}

func (e *Entry) String() string {
	return fmt.Sprintf(
		"name: '%v' type: %v modTime: %v size: %v oid: '%v' uid: %v gid: %v",
		e.Name, e.Type, e.ModTime, e.Size, e.ObjectID, e.UserID, e.GroupID,
	)
}
