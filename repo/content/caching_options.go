package content

import (
	"path/filepath"
	"time"
)

// DurationSeconds represents the duration in seconds.
type DurationSeconds float64

// DurationOrDefault returns the duration or the provided default if not set or zero.
func (s DurationSeconds) DurationOrDefault(def time.Duration) time.Duration {
	if s == 0 {
		return def
	}

	return time.Duration(float64(s) * float64(time.Second))
}

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory              string          `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes           int64           `json:"maxCacheSize,omitempty"`
	ContentCacheSizeLimitBytes  int64           `json:"contentCacheSizeLimitBytes,omitempty"`
	MaxMetadataCacheSizeBytes   int64           `json:"maxMetadataCacheSize,omitempty"`
	MetadataCacheSizeLimitBytes int64           `json:"metadataCacheSizeLimitBytes,omitempty"`
	MaxListCacheDuration        DurationSeconds `json:"maxListCacheDuration,omitempty"`
	MinMetadataSweepAge         DurationSeconds `json:"minMetadataSweepAge,omitempty"`
	MinContentSweepAge          DurationSeconds `json:"minContentSweepAge,omitempty"`
	MinIndexSweepAge            DurationSeconds `json:"minIndexSweepAge,omitempty"`
	HMACSecret                  []byte          `json:"-"`
}

// CloneOrDefault returns a clone of the caching options or empty options for nil.
func (c *CachingOptions) CloneOrDefault() *CachingOptions {
	if c == nil {
		return &CachingOptions{}
	}

	c2 := *c

	return &c2
}

// CacheSubdirOrEmpty returns path to a cache subdirectory or empty string if cache is disabled.
func (c *CachingOptions) CacheSubdirOrEmpty(subdir string) string {
	if c == nil {
		return ""
	}

	if c.CacheDirectory == "" {
		return ""
	}

	return filepath.Join(c.CacheDirectory, subdir)
}
