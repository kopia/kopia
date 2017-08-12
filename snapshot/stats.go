package snapshot

import "github.com/kopia/kopia/repo"

// Stats keeps track of snapshot generation statistics.
type Stats struct {
	Repository repo.Stats `json:"repo,omitempty"`

	TotalDirectoryCount int   `json:"dirCount"`
	TotalFileCount      int   `json:"fileCount"`
	TotalFileSize       int64 `json:"totalSize"`
	TotalBundleCount    int   `json:"bundleCount"`
	TotalBundleSize     int64 `json:"totalBundleSize"`

	CachedFiles    int `json:"cachedFiles"`
	NonCachedFiles int `json:"nonCachedFiles"`
}
