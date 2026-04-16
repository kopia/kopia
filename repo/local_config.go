package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
)

const configDirMode = 0o700

// ErrCannotWriteToRepoConnectionWithPermissiveCacheLoading error to indicate.
var ErrCannotWriteToRepoConnectionWithPermissiveCacheLoading = errors.New("cannot write to repo connection with permissive cache loading")

// ClientOptions contains client-specific options that are persisted in local configuration file.
type ClientOptions struct {
	Hostname string `json:"hostname"`
	Username string `json:"username"`

	ReadOnly               bool `json:"readonly,omitempty"`
	PermissiveCacheLoading bool `json:"permissiveCacheLoading,omitempty"`

	// Description is human-readable description of the repository to use in the UI.
	Description string `json:"description,omitempty"`

	EnableActions bool `json:"enableActions"`

	FormatBlobCacheDuration time.Duration `json:"formatBlobCacheDuration,omitempty"`

	Throttling *throttling.Limits `json:"throttlingLimits,omitempty"`
}

// ApplyDefaults returns a copy of ClientOptions with defaults filled out.
func (o ClientOptions) ApplyDefaults(ctx context.Context, defaultDesc string) ClientOptions {
	if o.Hostname == "" {
		o.Hostname = GetDefaultHostName(ctx)
	}

	if o.Username == "" {
		o.Username = GetDefaultUserName(ctx)
	}

	if o.Description == "" {
		o.Description = defaultDesc
	}

	if o.FormatBlobCacheDuration == 0 {
		o.FormatBlobCacheDuration = format.DefaultRepositoryBlobCacheDuration
	}

	return o
}

// Override returns ClientOptions that overrides fields present in the provided ClientOptions.
func (o ClientOptions) Override(other ClientOptions) ClientOptions {
	if other.Description != "" {
		o.Description = other.Description
	}

	if other.Hostname != "" {
		o.Hostname = other.Hostname
	}

	if other.Username != "" {
		o.Username = other.Username
	}

	if other.ReadOnly {
		o.ReadOnly = other.ReadOnly
	}

	return o
}

// UsernameAtHost returns 'username@hostname' string.
func (o ClientOptions) UsernameAtHost() string {
	return o.Username + "@" + o.Hostname
}

// LocalConfig is a configuration of Kopia stored in a configuration file.
type LocalConfig struct {
	// APIServer is only provided for remote repository.
	APIServer *APIServerInfo `json:"apiServer,omitempty"`

	// Storage is only provided for direct repository access.
	Storage *blob.ConnectionInfo `json:"storage,omitempty"`

	Caching *content.CachingOptions `json:"caching,omitempty"`

	ClientOptions
}

// writeToFile writes the config to a given file.
func (lc *LocalConfig) writeToFile(filename string) error {
	lc2 := *lc

	if lc.Caching != nil {
		lc2.Caching = lc.Caching.CloneOrDefault()

		// try computing relative pathname from config dir to the cache dir.
		d, err := filepath.Rel(filepath.Dir(filename), lc.Caching.CacheDirectory)
		if err == nil {
			lc2.Caching.CacheDirectory = d
		}
	}

	b, err := json.MarshalIndent(lc2, "", "  ")
	if err != nil {
		return errors.Wrap(err, "error creating config file contents")
	}

	if err = os.MkdirAll(filepath.Dir(filename), configDirMode); err != nil {
		return errors.Wrap(err, "unable to create config directory")
	}

	return errors.Wrap(atomicfile.Write(filename, bytes.NewReader(b)), "error writing file")
}

// LoadConfigFromFile reads the local configuration from the specified file.
func LoadConfigFromFile(fileName string) (*LocalConfig, error) {
	f, err := os.Open(fileName) //nolint:gosec
	if err != nil {
		return nil, errors.Wrap(err, "error loading config file")
	}
	defer f.Close() //nolint:errcheck

	var lc LocalConfig

	if err := json.NewDecoder(f).Decode(&lc); err != nil {
		return nil, errors.Wrap(err, "error decoding config json")
	}

	// cache directory is stored as relative to config file name, resolve it to absolute.
	if lc.Caching != nil {
		if lc.Caching.CacheDirectory != "" && !ospath.IsAbs(lc.Caching.CacheDirectory) {
			lc.Caching.CacheDirectory = filepath.Join(filepath.Dir(fileName), lc.Caching.CacheDirectory)
		}

		// override cache directory from the environment variable.
		if cd := os.Getenv("KOPIA_CACHE_DIRECTORY"); cd != "" && ospath.IsAbs(cd) {
			lc.Caching.CacheDirectory = cd
		}
	}

	if lc.PermissiveCacheLoading && os.Getenv("KOPIA_UPGRADE_LOCK_ENABLED") == "" {
		return nil, errors.New("must have set KOPIA_UPGRADE_LOCK_ENABLED when connecting to repository with permissive cache loading")
	}

	return &lc, nil
}
