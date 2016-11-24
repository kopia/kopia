package repofs

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
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
	return fmt.Sprintf("%v@%v:%v", ssi.UserName, ssi.Host, ssi.Path)
}

// ParseSourceSnashotInfo parses a given path in the context of given hostname and username and returns
// SnapshotSourceInfo. The path may be bare (in which case it's interpreted as local path and canonicalized)
// or may be 'username@host:path' where path, username and host are not processed.
func ParseSourceSnashotInfo(path string, hostname string, username string) (SnapshotSourceInfo, error) {
	p1 := strings.Index(path, "@")
	p2 := strings.Index(path, ":")

	if p1 > 0 && p2 > 0 && p1 < p2 && p2 < len(path) {
		return SnapshotSourceInfo{
			UserName: path[0:p1],
			Host:     path[p1+1 : p2],
			Path:     path[p2+1:],
		}, nil
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return SnapshotSourceInfo{}, fmt.Errorf("invalid directory: '%s': %s", path, err)
	}

	return SnapshotSourceInfo{
		Host:     hostname,
		UserName: username,
		Path:     filepath.Clean(absPath),
	}, nil
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
