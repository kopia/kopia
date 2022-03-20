package snapshot

import (
	"sync/atomic"

	"github.com/kopia/kopia/fs"
)

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	// keep all int64 aligned because they will be atomically updated
	// +checkatomic
	TotalFileSize int64 `json:"totalSize"`
	// +checkatomic
	ExcludedTotalFileSize int64 `json:"excludedTotalSize"`

	// keep all int32 aligned because they will be atomically updated
	// +checkatomic
	TotalFileCount int32 `json:"fileCount"`
	// +checkatomic
	CachedFiles int32 `json:"cachedFiles"`
	// +checkatomic
	NonCachedFiles int32 `json:"nonCachedFiles"`

	// +checkatomic
	TotalDirectoryCount int32 `json:"dirCount"`

	// +checkatomic
	ExcludedFileCount int32 `json:"excludedFileCount"`
	// +checkatomic
	ExcludedDirCount int32 `json:"excludedDirCount"`

	// +checkatomic
	IgnoredErrorCount int32 `json:"ignoredErrorCount"`
	// +checkatomic
	ErrorCount int32 `json:"errorCount"`
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
