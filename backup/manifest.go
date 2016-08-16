package backup

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"time"

	"github.com/kopia/kopia/repo"
)

// Manifest stores information about single backup.
type Manifest struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	HostName    string `json:"host"`
	UserName    string `json:"userName"`
	Description string `json:"description"`

	Handle       string        `json:"handle"`
	Source       string        `json:"source"`
	RootObjectID repo.ObjectID `json:"root"`
	HashCacheID  repo.ObjectID `json:"hashCache"`

	FileCount      int64 `json:"fileCount"`
	DirectoryCount int64 `json:"dirCount"`
	TotalFileSize  int64 `json:"totalSize"`
}

// SourceID generates unique identifier of the backup source, which is a
// SHA1 hash of the host name, username and source directory.
func (m Manifest) SourceID() string {
	h := sha1.New()
	io.WriteString(h, m.HostName)
	h.Write(zeroByte)
	io.WriteString(h, m.UserName)
	h.Write(zeroByte)
	io.WriteString(h, m.Source)
	h.Write(zeroByte)
	return hex.EncodeToString(h.Sum(nil))
}
