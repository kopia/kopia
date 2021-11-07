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

	if opt.MaxCacheSizeBytes == 0 {
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

	lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
	lc.Caching.MaxMetadataCacheSizeBytes = opt.MaxMetadataCacheSizeBytes
	lc.Caching.MaxListCacheDurationSec = opt.MaxListCacheDurationSec

	log(ctx).Debugw("creating cache directory", "directory", lc.Caching.CacheDirectory, "maxSize", lc.Caching.MaxCacheSizeBytes)

	return nil
}
