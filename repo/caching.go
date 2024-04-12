package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

// GetCachingOptions reads caching configuration for a given repository.
func GetCachingOptions(ctx context.Context, configFile string) (*content.CachingOptions, error) {
	lc, err := LoadConfigFromFile(configFile)
	if err != nil {
		return nil, err
	}

	return lc.Caching.CloneOrDefault(), nil
}

// SetCachingOptions changes caching configuration for a given repository.
func SetCachingOptions(ctx context.Context, configFile string, opt *content.CachingOptions) error {
	lc, err := LoadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	if err = setupCachingOptionsWithDefaults(ctx, configFile, lc, opt, nil); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	return lc.writeToFile(configFile)
}

func setupCachingOptionsWithDefaults(ctx context.Context, configPath string, lc *LocalConfig, opt *content.CachingOptions, uniqueID []byte) error {
	opt = opt.CloneOrDefault()

	if opt.ContentCacheSizeBytes == 0 {
		lc.Caching = &content.CachingOptions{}
		return nil
	}

	if lc.Caching == nil {
		lc.Caching = &content.CachingOptions{}
	}

	if opt.CacheDirectory == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return errors.Wrap(err, "unable to determine cache directory")
		}

		h := sha256.New()
		h.Write(uniqueID)
		h.Write([]byte(configPath))
		lc.Caching.CacheDirectory = filepath.Join(cacheDir, "kopia", hex.EncodeToString(h.Sum(nil))[0:16])
	} else {
		d, err := filepath.Abs(opt.CacheDirectory)
		if err != nil {
			return errors.Wrap(err, "unable to determine absolute cache path")
		}

		lc.Caching.CacheDirectory = d
	}

	lc.Caching.ContentCacheSizeBytes = opt.ContentCacheSizeBytes
	lc.Caching.ContentCacheSizeLimitBytes = opt.ContentCacheSizeLimitBytes
	lc.Caching.MetadataCacheSizeBytes = opt.MetadataCacheSizeBytes
	lc.Caching.MetadataCacheSizeLimitBytes = opt.MetadataCacheSizeLimitBytes
	lc.Caching.MaxListCacheDuration = opt.MaxListCacheDuration
	lc.Caching.MinContentSweepAge = opt.MinContentSweepAge
	lc.Caching.MinMetadataSweepAge = opt.MinMetadataSweepAge
	lc.Caching.MinIndexSweepAge = opt.MinIndexSweepAge

	log(ctx).Debugf("Creating cache directory '%v' with max size %v", lc.Caching.CacheDirectory, lc.Caching.ContentCacheSizeBytes)

	return nil
}
