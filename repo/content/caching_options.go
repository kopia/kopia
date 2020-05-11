package content

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory            string `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes         int64  `json:"maxCacheSize,omitempty"`
	MaxMetadataCacheSizeBytes int64  `json:"maxMetadataCacheSize,omitempty"`
	MaxListCacheDurationSec   int    `json:"maxListCacheDuration,omitempty"`
	IgnoreListCache           bool   `json:"-"`
	HMACSecret                []byte `json:"-"`

	ownWritesCache ownWritesCache
}
