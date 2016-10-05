package repofs

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
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

func (ssi SnapshotSourceInfo) String() string {
	return fmt.Sprintf("%v@%v : %v", ssi.UserName, ssi.Host, ssi.Path)
}

// HashString generates hash of SnapshotSourceInfo.
func (ssi SnapshotSourceInfo) HashString() string {
	h := sha1.New()
	io.WriteString(h, ssi.Host)
	h.Write(zeroByte)
	io.WriteString(h, ssi.UserName)
	h.Write(zeroByte)
	io.WriteString(h, ssi.Path)
	h.Write(zeroByte)
	return hex.EncodeToString(h.Sum(nil))
}

// SnapshotStats keeps track of snapshot generation statistics.
type SnapshotStats struct {
	Repository *repo.Stats `json:"repo,omitempty"`

	TotalDirectoryCount int   `json:"dirCount"`
	TotalFileCount      int   `json:"fileCount"`
	TotalFileSize       int64 `json:"totalSize"`
	TotalBundleCount    int   `json:"bundleCount"`
	TotalBundleSize     int64 `json:"totalBundleSize"`

	CachedDirectories    int `json:"cachedDirectories"`
	CachedFiles          int `json:"cachedFiles"`
	NonCachedDirectories int `json:"nonCachedDirectories"`
	NonCachedFiles       int `json:"nonCachedFiles"`
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

	Stats SnapshotStats `json:"stats"`
}
