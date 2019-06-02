package snapshot

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/content"
)

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	Content content.Stats `json:"content,omitempty"`

	TotalDirectoryCount int   `json:"dirCount"`
	TotalFileCount      int   `json:"fileCount"`
	TotalFileSize       int64 `json:"totalSize"`

	ExcludedFileCount     int   `json:"excludedFileCount"`
	ExcludedTotalFileSize int64 `json:"excludedTotalSize"`
	ExcludedDirCount      int   `json:"excludedDirCount"`

	CachedFiles    int `json:"cachedFiles"`
	NonCachedFiles int `json:"nonCachedFiles"`

	ReadErrors int `json:"readErrors"`
}

// AddExcluded adds the information about excluded file to the statistics.
func (s *Stats) AddExcluded(md fs.Entry) {
	if md.IsDir() {
		s.ExcludedDirCount++
	} else {
		s.ExcludedFileCount++
		s.ExcludedTotalFileSize += md.Size()
	}
}
