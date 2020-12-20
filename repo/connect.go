package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	PersistCredentials bool `json:"persistCredentials"`
	ClientOptions

	content.CachingOptions
}

// ErrRepositoryNotInitialized is returned when attempting to connect to repository that has not
// been initialized.
var ErrRepositoryNotInitialized = errors.Errorf("repository not initialized in the provided storage")

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st blob.Storage, password string, opt *ConnectOptions) error {
	if opt == nil {
		opt = &ConnectOptions{}
	}

	formatBytes, err := st.GetBlob(ctx, FormatBlobID, 0, -1)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			// nolint:wrapcheck
			return ErrRepositoryNotInitialized
		}

		return errors.Wrap(err, "unable to read format blob")
	}

	f, err := parseFormatBlob(formatBytes)
	if err != nil {
		return err
	}

	var lc LocalConfig

	ci := st.ConnectionInfo()
	lc.Storage = &ci
	lc.ClientOptions = opt.ClientOptions.ApplyDefaults(ctx, "Repository in "+st.DisplayName())

	if err = setupCaching(ctx, configFile, &lc, &opt.CachingOptions, f.UniqueID); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return errors.Wrap(err, "unable to serialize JSON")
	}

	if err = os.MkdirAll(filepath.Dir(configFile), 0o700); err != nil {
		return errors.Wrap(err, "unable to create config directory")
	}

	if err = ioutil.WriteFile(configFile, d, 0o600); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return verifyConnect(ctx, configFile, password, opt.PersistCredentials)
}

func verifyConnect(ctx context.Context, configFile, password string, persist bool) error {
	// now verify that the repository can be opened with the provided config file.
	r, err := Open(ctx, configFile, password, nil)
	if err != nil {
		// we failed to open the repository after writing the config file,
		// remove the config file we just wrote and any caches.
		if derr := Disconnect(ctx, configFile); derr != nil {
			log(ctx).Warningf("unable to disconnect after unsuccessful opening: %v", derr)
		}

		return err
	}

	if persist {
		if err := persistPassword(ctx, configFile, password); err != nil {
			return errors.Wrap(err, "unable to persist password")
		}
	} else {
		deletePassword(ctx, configFile)
	}

	return r.Close(ctx)
}

func setupCaching(ctx context.Context, configPath string, lc *LocalConfig, opt *content.CachingOptions, uniqueID []byte) error {
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
		h.Write(uniqueID)           //nolint:errcheck
		h.Write([]byte(configPath)) //nolint:errcheck
		opt.CacheDirectory = filepath.Join(cacheDir, "kopia", hex.EncodeToString(h.Sum(nil))[0:16])
	}

	var err error

	// try computing relative pathname from config dir to the cache dir.
	lc.Caching.CacheDirectory, err = filepath.Rel(filepath.Dir(configPath), opt.CacheDirectory)

	if err != nil {
		// fall back to storing absolute path
		lc.Caching.CacheDirectory, err = filepath.Abs(opt.CacheDirectory)
	}

	if err != nil {
		return errors.Wrap(err, "error computing cache directory")
	}

	lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
	lc.Caching.MaxMetadataCacheSizeBytes = opt.MaxMetadataCacheSizeBytes
	lc.Caching.MaxListCacheDurationSec = opt.MaxListCacheDurationSec

	log(ctx).Debugf("Creating cache directory '%v' with max size %v", lc.Caching.CacheDirectory, lc.Caching.MaxCacheSizeBytes)

	return nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(ctx context.Context, configFile string) error {
	cfg, err := loadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	deletePassword(ctx, configFile)

	if cfg.Caching != nil && cfg.Caching.CacheDirectory != "" {
		if err = os.RemoveAll(cfg.Caching.CacheDirectory); err != nil {
			log(ctx).Warningf("unable to remove cache directory: %v", err)
		}
	}

	maintenanceLock := configFile + ".mlock"
	if err := os.RemoveAll(maintenanceLock); err != nil {
		log(ctx).Warningf("unable to remove maintenance lock file", maintenanceLock)
	}

	return os.Remove(configFile)
}

// SetClientOptions updates client options stored in the provided configuration file.
func SetClientOptions(ctx context.Context, configFile string, cliOpt ClientOptions) error {
	lc, err := loadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	lc.ClientOptions = cliOpt

	d, err := json.MarshalIndent(lc, "", "  ")
	if err != nil {
		return errors.Wrap(err, "error marshaling config JSON")
	}

	if err = ioutil.WriteFile(configFile, d, 0o600); err != nil {
		return errors.Wrap(err, "unable to write config file")
	}

	return nil
}
