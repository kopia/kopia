package repo

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
)

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	ClientOptions

	content.CachingOptions
}

// ErrRepositoryNotInitialized is returned when attempting to connect to repository that has not
// been initialized.
var ErrRepositoryNotInitialized = errors.New("repository not initialized in the provided storage")

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st blob.Storage, password string, opt *ConnectOptions) error {
	if opt == nil {
		opt = &ConnectOptions{}
	}

	var formatBytes gather.WriteBuffer
	defer formatBytes.Close()

	if err := st.GetBlob(ctx, format.KopiaRepositoryBlobID, 0, -1, &formatBytes); err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			return ErrRepositoryNotInitialized
		}

		return errors.Wrap(err, "unable to read format blob")
	}

	f, err := format.ParseKopiaRepositoryJSON(formatBytes.ToByteSlice())
	if err != nil {
		//nolint:wrapcheck
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

	return verifyConnect(ctx, configFile, password)
}

func verifyConnect(ctx context.Context, configFile, password string) error {
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

	return errors.Wrap(r.Close(ctx), "error closing repository")
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(ctx context.Context, configFile string) error {
	cfg, err := LoadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	if cfg.Caching != nil && cfg.Caching.CacheDirectory != "" {
		if !ospath.IsAbs(cfg.Caching.CacheDirectory) {
			return errors.New("cache directory was not absolute, refusing to delete")
		}

		if err = os.RemoveAll(cfg.Caching.CacheDirectory); err != nil {
			log(ctx).Errorf("unable to remove cache directory: %v", err)
		}
	}

	maintenanceLock := configFile + ".mlock"
	if err := os.RemoveAll(maintenanceLock); err != nil {
		log(ctx).Error("unable to remove maintenance lock file", maintenanceLock)
	}

	//nolint:wrapcheck
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
