package repofs

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"time"

	"github.com/kopia/kopia/repo"
)

var zeroByte = []byte{0}

// SnapshotSourceInfo represents the information about snapshot source.
type SnapshotSourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

// HashString generates hash of SnapshotSourceInfo.
func (r SnapshotSourceInfo) HashString() string {
	h := sha1.New()
	io.WriteString(h, r.Host)
	h.Write(zeroByte)
	io.WriteString(h, r.UserName)
	h.Write(zeroByte)
	io.WriteString(h, r.Path)
	h.Write(zeroByte)
	return hex.EncodeToString(h.Sum(nil))
}

// Snapshot stores information about a single filesystem snapshot.
type Snapshot struct {
	Source SnapshotSourceInfo `json:"source"`

	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`

	Handle       string        `json:"handle"`
	RootObjectID repo.ObjectID `json:"root"`
	HashCacheID  repo.ObjectID `json:"hashCache"`

	FileCount      int64 `json:"fileCount"`
	DirectoryCount int64 `json:"dirCount"`
	TotalFileSize  int64 `json:"totalSize"`

	Stats *repo.Stats `json:"stats"`
}
