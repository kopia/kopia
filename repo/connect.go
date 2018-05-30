package repo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/storage/filesystem"
	_ "github.com/kopia/kopia/storage/gcs"
)

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	PersistCredentials bool
	block.CachingOptions
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, creds auth.Credentials, opt ConnectOptions) error {
	formatBytes, err := st.GetBlock(ctx, FormatBlockID, 0, -1)
	if err != nil {
		return fmt.Errorf("unable to read format block: %v", err)
	}

	f, err := parseFormatBlock(formatBytes)
	if err != nil {
		return err
	}

	masterKey, err := creds.GetMasterKey(f.SecurityOptions)
	if err != nil {
		return fmt.Errorf("can't get master key: %v", err)
	}

	cfg := &config.RepositoryConnectionInfo{
		ConnectionInfo: st.ConnectionInfo(),
		Key:            masterKey,
	}

	var lc config.LocalConfig
	lc.Connection = cfg

	if !opt.PersistCredentials {
		lc.Connection.Key = nil
	}

	if err = setupCaching(&lc, opt.CachingOptions, f.UniqueID); err != nil {
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
	r, err := Open(ctx, configFile, &Options{
		Credentials: creds,
	})
	if err != nil {
		return err
	}

	return r.Close(ctx)
}

func setupCaching(lc *config.LocalConfig, opt block.CachingOptions, uniqueID []byte) error {
	if opt.MaxCacheSizeBytes == 0 {
		lc.Caching = block.CachingOptions{}
		return nil
	}

	if opt.CacheDirectory == "" {
		lc.Caching.CacheDirectory = filepath.Join(ospath.CacheDir(), hex.EncodeToString(uniqueID))
	} else {
		absCacheDir, err := filepath.Abs(opt.CacheDirectory)
		if err != nil {
			return err
		}

		lc.Caching.CacheDirectory = absCacheDir
	}
	lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
	lc.Caching.MaxListCacheDurationSec = opt.MaxListCacheDurationSec

	log.Printf("Creating cache directory '%v' with max size %v", lc.Caching.CacheDirectory, units.BytesStringBase2(lc.Caching.MaxCacheSizeBytes))
	if err := os.MkdirAll(lc.Caching.CacheDirectory, 0700); err != nil {
		log.Warn().Err(err).Msg("unablet to create cache directory")
	}
	return nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	cfg, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	if cfg.Caching.CacheDirectory != "" {
		if err = os.RemoveAll(cfg.Caching.CacheDirectory); err != nil {
			log.Warn().Err(err).Msg("unable to to remove cache directory")
		}
	}

	return os.Remove(configFile)
}
