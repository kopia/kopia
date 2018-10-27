package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/repo/block"
	"github.com/kopia/repo/storage"
)

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	block.CachingOptions
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, password string, opt ConnectOptions) error {
	formatBytes, err := st.GetBlock(ctx, FormatBlockID, 0, -1)
	if err != nil {
		return fmt.Errorf("unable to read format block: %v", err)
	}

	f, err := parseFormatBlock(formatBytes)
	if err != nil {
		return err
	}

	var lc LocalConfig
	lc.Storage = st.ConnectionInfo()

	if err = setupCaching(configFile, &lc, opt.CachingOptions, f.UniqueID); err != nil {
		return fmt.Errorf("unable to set up caching: %v", err)
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(configFile), 0700); err != nil {
		return fmt.Errorf("unable to create config directory: %v", err)
	}

	if err = ioutil.WriteFile(configFile, d, 0600); err != nil {
		return fmt.Errorf("unable to write config file: %v", err)
	}

	// now verify that the repository can be opened with the provided config file.
	r, err := Open(ctx, configFile, password, nil)
	if err != nil {
		return err
	}

	return r.Close(ctx)
}

func setupCaching(configPath string, lc *LocalConfig, opt block.CachingOptions, uniqueID []byte) error {
	if opt.MaxCacheSizeBytes == 0 {
		lc.Caching = block.CachingOptions{}
		return nil
	}

	if opt.CacheDirectory == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("unable to determine cache directory: %v", err)
		}

		h := sha256.New()
		h.Write(uniqueID)
		h.Write([]byte(configPath))
		lc.Caching.CacheDirectory = filepath.Join(cacheDir, "kopia", hex.EncodeToString(h.Sum(nil))[0:16])
	} else {
		absCacheDir, err := filepath.Abs(opt.CacheDirectory)
		if err != nil {
			return err
		}

		lc.Caching.CacheDirectory = absCacheDir
	}
	lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
	lc.Caching.MaxListCacheDurationSec = opt.MaxListCacheDurationSec

	log.Debugf("Creating cache directory '%v' with max size %v", lc.Caching.CacheDirectory, lc.Caching.MaxCacheSizeBytes)
	if err := os.MkdirAll(lc.Caching.CacheDirectory, 0700); err != nil {
		log.Warningf("unablet to create cache directory: %v", err)
	}
	return nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	cfg, err := loadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	if cfg.Caching.CacheDirectory != "" {
		if err = os.RemoveAll(cfg.Caching.CacheDirectory); err != nil {
			log.Warningf("unable to to remove cache directory: %v", err)
		}
	}

	return os.Remove(configFile)
}
