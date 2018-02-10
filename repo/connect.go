package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/auth"
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

	CacheDirectory    string
	MaxCacheSizeBytes int64
	MaxListDuration   time.Duration
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, creds auth.Credentials, opt ConnectOptions) error {
	formatBytes, err := st.GetBlock(FormatBlockID, 0, -1)
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

	if opt.MaxCacheSizeBytes > 0 {
		if opt.CacheDirectory == "" {
			lc.Caching.CacheDirectory = filepath.Join(ospath.CacheDir(), fmt.Sprintf("%x", f.UniqueID))
		} else {
			absCacheDir, err := filepath.Abs(opt.CacheDirectory)
			if err != nil {
				return err
			}

			lc.Caching.CacheDirectory = absCacheDir
		}
		lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
		lc.Caching.MaxListCacheDurationSec = int(opt.MaxListDuration.Seconds())

		log.Printf("Creating cache directory '%v' with max size %v", lc.Caching.CacheDirectory, units.BytesStringBase2(lc.Caching.MaxCacheSizeBytes))
		os.MkdirAll(lc.Caching.CacheDirectory, 0700)
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(configFile), 0700); err != nil {
		return err
	}

	if err := ioutil.WriteFile(configFile, d, 0600); err != nil {
		return nil
	}

	// now verify that the repository can be opened with the provided config file.
	r, err := Open(ctx, configFile, &Options{
		CredentialsCallback: func() (auth.Credentials, error) {
			return creds, nil
		},
	})
	if err != nil {
		return err
	}

	r.Close()
	return nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	_, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	return os.Remove(configFile)
}
