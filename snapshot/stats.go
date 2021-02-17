package snapshot

import (
	"sync/atomic"

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

	TotalDirectoryCount int32 `json:"dirCount"`

	ExcludedFileCount int32 `json:"excludedFileCount"`
	ExcludedDirCount  int32 `json:"excludedDirCount"`

	IgnoredErrorCount int32 `json:"ignoredErrorCount"`
	ErrorCount        int32 `json:"errorCount"`
}

// AddExcluded adds the information about excluded file to the statistics.
func (s *Stats) AddExcluded(md fs.Entry) {
	if md.IsDir() {
		atomic.AddInt32(&s.ExcludedDirCount, 1)
	} else {
		atomic.AddInt32(&s.ExcludedFileCount, 1)
		atomic.AddInt64(&s.ExcludedTotalFileSize, md.Size())
	}
}
