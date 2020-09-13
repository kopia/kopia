package snapshot

import (
	"github.com/kopia/kopia/fs"
)

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	// keep all int64 aligned because they will be atomically updated
	TotalFileSize         int64 `json:"totalSize"`
	ExcludedTotalFileSize int64 `json:"excludedTotalSize"`

	// keep all int32 aligned because they will be atomically updated
	TotalFileCount int32 `json:"fileCount"`
	CachedFiles    int32 `json:"cachedFiles"`
	NonCachedFiles int32 `json:"nonCachedFiles"`

	TotalDirectoryCount int `json:"dirCount"`

	ExcludedFileCount int `json:"excludedFileCount"`
	ExcludedDirCount  int `json:"excludedDirCount"`

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
