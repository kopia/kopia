package repo

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

var (
	log = repologging.Logger("kopia/repo")
)

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage         func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function
	ObjectManagerOptions object.ManagerOptions
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = errors.Errorf("invalid repository password")

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile, password string, options *Options) (rep *Repository, err error) {
	defer func() {
		if err != nil {
			log.Errorf("failed to open repository: %v", err)
		}
	}()

	if options == nil {
		options = &Options{}
	}

	configFile, err = filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}

	lc, err := loadConfigFromFile(configFile)
	if err != nil {
		return nil, err
	}

	st, err := blob.NewStorage(ctx, lc.Storage)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open storage")
	}

	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	r, err := OpenWithConfig(ctx, st, lc, password, options, lc.Caching)
	if err != nil {
		st.Close(ctx) //nolint:errcheck
		return nil, err
	}

	r.ConfigFile = configFile

	return r, nil
}

// OpenWithConfig opens the repository with a given configuration, avoiding the need for a config file.
func OpenWithConfig(ctx context.Context, st blob.Storage, lc *LocalConfig, password string, options *Options, caching content.CachingOptions) (*Repository, error) {
	// Read format blob, potentially from cache.
	fb, err := readAndCacheFormatBlobBytes(ctx, st, caching.CacheDirectory)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read format blob")
	}

	f, err := parseFormatBlob(fb)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse format blob")
	}

	fb, err = addFormatBlobChecksumAndLength(fb)
	if err != nil {
		return nil, errors.Errorf("unable to add checksum")
	}

	masterKey, err := f.deriveMasterKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	repoConfig, err := f.decryptFormatBytes(masterKey)
	if err != nil {
		return nil, ErrInvalidPassword
	}

	caching.HMACSecret = deriveKeyFromMasterKey(masterKey, f.UniqueID, []byte("local-cache-integrity"), 16)

	fo := &repoConfig.FormattingOptions

	if fo.MaxPackSize == 0 {
		// legacy only, apply default
		fo.MaxPackSize = 20 << 20 // nolint:gomnd
	}

	cm, err := content.NewManager(ctx, st, fo, caching, fb)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open content manager")
	}

	om, err := object.NewObjectManager(ctx, cm, repoConfig.Format, options.ObjectManagerOptions)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open object manager")
	}

	manifests, err := manifest.NewManager(ctx, cm)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open manifests")
	}

	return &Repository{
		Content:   cm,
		Objects:   om,
		Blobs:     st,
		Manifests: manifests,
		UniqueID:  f.UniqueID,

		formatBlob: f,
		masterKey:  masterKey,
	}, nil
}

// SetCachingConfig changes caching configuration for a given repository.
func (r *Repository) SetCachingConfig(opt content.CachingOptions) error {
	lc, err := loadConfigFromFile(r.ConfigFile)
	if err != nil {
		return err
	}

	if err = setupCaching(r.ConfigFile, lc, opt, r.UniqueID); err != nil {
		return errors.Wrap(err, "unable to set up caching")
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(r.ConfigFile, d, 0600); err != nil {
		return nil
	}

	return nil
}

func readAndCacheFormatBlobBytes(ctx context.Context, st blob.Storage, cacheDirectory string) ([]byte, error) {
	cachedFile := filepath.Join(cacheDirectory, "kopia.repository")

	if cacheDirectory != "" {
		b, err := ioutil.ReadFile(cachedFile) //nolint:gosec
		if err == nil {
			// read from cache.
			return b, nil
		}
	}

	b, err := st.GetBlob(ctx, FormatBlobID, 0, -1)
	if err != nil {
		return nil, err
	}

	if cacheDirectory != "" {
		if err := ioutil.WriteFile(cachedFile, b, 0600); err != nil {
			log.Warningf("warning: unable to write cache: %v", err)
		}
	}

	return b, nil
}
