package snapshot

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/repo/block"
)

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	Block block.Stats `json:"repo,omitempty"`

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
func (s *Stats) AddExcluded(md *fs.EntryMetadata) {
	if md.FileMode().IsDir() {
		s.ExcludedDirCount++
	} else {
		s.ExcludedFileCount++
		s.ExcludedTotalFileSize += md.FileSize
	}
}
