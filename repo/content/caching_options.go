package content

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory            string `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes         int64  `json:"maxCacheSize,omitempty"`
	MaxMetadataCacheSizeBytes int64  `json:"maxMetadataCacheSize,omitempty"`
	MaxListCacheDurationSec   int    `json:"maxListCacheDuration,omitempty"`
	HMACSecret                []byte `json:"-"`

	ownWritesCache ownWritesCache
}

// CloneOrDefault returns a clone of the caching options or empty options for nil.
func (c *CachingOptions) CloneOrDefault() *CachingOptions {
	if c == nil {
		return &CachingOptions{}
	}

	c2 := *c

	return &c2
}
