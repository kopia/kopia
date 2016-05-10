package fs

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"
)

// Entry stores attributes of a single entry in a directory.
type Entry struct {
	Name     string
	FileMode os.FileMode
	FileSize int64
	ModTime  time.Time
	OwnerID  uint32
	GroupID  uint32
	ObjectID repo.ObjectID
}

func isLess(name1, name2 string) bool {
	if name1 == name2 {
		return false
	}

	return isLessOrEqual(name1, name2)
}

func split1(name string) (head, tail string) {
	n := strings.IndexByte(name, '/')
	if n >= 0 {
		return name[0 : n+1], name[n+1:]
	}

	return name, ""
}

func isLessOrEqual(name1, name2 string) bool {
	parts1 := strings.Split(name1, "/")
	parts2 := strings.Split(name2, "/")

	i := 0
	for i < len(parts1) && i < len(parts2) {
		if parts1[i] == parts2[i] {
			i++
			continue
		}
		if parts1[i] == "" {
			return false
		}
		if parts2[i] == "" {
			return true
		}
		return parts1[i] < parts2[i]
	}

	return len(parts1) <= len(parts2)
}

func (e *Entry) metadataHash() uint64 {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, e.ModTime.UnixNano())
	binary.Write(h, binary.LittleEndian, e.FileMode)
	if e.FileMode.IsRegular() {
		binary.Write(h, binary.LittleEndian, e.FileSize)
	}
	binary.Write(h, binary.LittleEndian, e.OwnerID)
	binary.Write(h, binary.LittleEndian, e.GroupID)
	return h.Sum64()
}
