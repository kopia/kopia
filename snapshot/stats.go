package snapshot

import "github.com/kopia/kopia/block"

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	Block block.Stats `json:"repo,omitempty"`

	TotalDirectoryCount int   `json:"dirCount"`
	TotalFileCount      int   `json:"fileCount"`
	TotalFileSize       int64 `json:"totalSize"`

	ExcludedFileCount     int   `json:"excludedFileCount"`
	ExcludedTotalFileSize int64 `json:"excludedTotalSize"`

	CachedFiles    int `json:"cachedFiles"`
	NonCachedFiles int `json:"nonCachedFiles"`

	ReadErrors int `json:"readErrors"`
}
