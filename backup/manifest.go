package backup

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"time"
)

// Manifest stores information about single backup.
type Manifest struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	HostName    string `json:"host"`
	UserName    string `json:"userName"`
	Description string `json:"description"`

	SourceDirectory string `json:"source"`
	RootObjectID    string `json:"root"`
	HashCacheID     string `json:"hashCache"`

	FileCount      int64 `json:"fileCount"`
	DirectoryCount int64 `json:"dirCount"`
	TotalFileSize  int64 `json:"totalSize"`
}

func (m Manifest) SourceID() string {
	h := sha1.New()
	io.WriteString(h, m.HostName)
	h.Write(zeroByte)
	io.WriteString(h, m.UserName)
	h.Write(zeroByte)
	io.WriteString(h, m.SourceDirectory)
	h.Write(zeroByte)
	return hex.EncodeToString(h.Sum(nil))
}
