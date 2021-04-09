package repo

import (
	"context"
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

	if err = setupCachingOptionsWithDefaults(ctx, configFile, &lc, &opt.CachingOptions, f.UniqueID); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	if err := lc.writeToFile(configFile); err != nil {
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
			log(ctx).Errorf("unable to disconnect after unsuccessful opening: %v", derr)
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

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(ctx context.Context, configFile string) error {
	cfg, err := LoadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	deletePassword(ctx, configFile)

	if cfg.Caching != nil && cfg.Caching.CacheDirectory != "" {
		if !filepath.IsAbs(cfg.Caching.CacheDirectory) {
			return errors.Errorf("cache directory was not absolute, refusing to delete")
		}

		if err = os.RemoveAll(cfg.Caching.CacheDirectory); err != nil {
			log(ctx).Errorf("unable to remove cache directory: %v", err)
		}
	}

	maintenanceLock := configFile + ".mlock"
	if err := os.RemoveAll(maintenanceLock); err != nil {
		log(ctx).Errorf("unable to remove maintenance lock file", maintenanceLock)
	}

	return os.Remove(configFile)
}

// SetClientOptions updates client options stored in the provided configuration file.
func SetClientOptions(ctx context.Context, configFile string, cliOpt ClientOptions) error {
	lc, err := LoadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	lc.ClientOptions = cliOpt

	return lc.writeToFile(configFile)
}
