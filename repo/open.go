package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/storage"
	"github.com/kopia/kopia/repo/storage/logging"
)

var log = kopialogging.Logger("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage         func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function
	ObjectManagerOptions object.ManagerOptions
}

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile string, password string, options *Options) (rep *Repository, err error) {
	log.Debugf("opening repository from %v", configFile)
	defer func() {
		if err == nil {
			log.Debugf("opened repository")
		} else {
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

	log.Debugf("loading config from file: %v", configFile)
	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	log.Debugf("opening storage: %v", lc.Storage.Type)

	st, err := storage.NewStorage(ctx, lc.Storage)
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %v", err)
	}

	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	r, err := connect(ctx, st, lc, password, options, lc.Caching)
	if err != nil {
		st.Close(ctx) //nolint:errcheck
		return nil, err
	}

	r.ConfigFile = configFile

	return r, nil
}

func connect(ctx context.Context, st storage.Storage, lc *config.LocalConfig, password string, options *Options, caching block.CachingOptions) (*Repository, error) {
	log.Debugf("reading encrypted format block")
	// Read cache block, potentially from cache.
	f, err := readAndCacheFormatBlock(ctx, st, caching.CacheDirectory)
	if err != nil {
		return nil, fmt.Errorf("unable to read format block: %v", err)
	}

	masterKey, err := f.deriveMasterKeyFromPassword(password)
	if err != nil {
		return nil, err
	}

	repoConfig, err := f.decryptFormatBytes(masterKey)
	if err != nil {
		return nil, fmt.Errorf("unable to decrypt repository config: %v", err)
	}

	caching.HMACSecret = deriveKeyFromMasterKey(masterKey, f.UniqueID, []byte("local-cache-integrity"), 16)

	fo := repoConfig.FormattingOptions
	if fo.MaxPackSize == 0 {
		fo.MaxPackSize = repoConfig.MaxBlockSize
	}

	log.Debugf("initializing block manager")
	bm, err := block.NewManager(ctx, st, fo, caching)
	if err != nil {
		return nil, fmt.Errorf("unable to open block manager: %v", err)
	}

	log.Debugf("initializing object manager")
	om, err := object.NewObjectManager(ctx, bm, *repoConfig, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	log.Debugf("initializing manifest manager")
	manifests, err := manifest.NewManager(ctx, bm)
	if err != nil {
		return nil, fmt.Errorf("unable to open manifests: %v", err)
	}

	return &Repository{
		Blocks:         bm,
		Objects:        om,
		Storage:        st,
		Manifests:      manifests,
		CacheDirectory: caching.CacheDirectory,
		UniqueID:       f.UniqueID,
	}, nil
}

// SetCachingConfig changes caching configuration for a given repository config file.
func SetCachingConfig(ctx context.Context, configFile string, opt block.CachingOptions) error {
	configFile, err := filepath.Abs(configFile)
	if err != nil {
		return err
	}

	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	st, err := storage.NewStorage(ctx, lc.Storage)
	if err != nil {
		return fmt.Errorf("cannot open storage: %v", err)
	}

	f, err := readAndCacheFormatBlock(ctx, st, "")
	if err != nil {
		return fmt.Errorf("can't read format block: %v", err)
	}

	if err = setupCaching(configFile, lc, opt, f.UniqueID); err != nil {
		return fmt.Errorf("unable to set up caching: %v", err)
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(configFile, d, 0600); err != nil {
		return nil
	}

	return nil
}

func readAndCacheFormatBlock(ctx context.Context, st storage.Storage, cacheDirectory string) (*formatBlock, error) {
	cachedFile := filepath.Join(cacheDirectory, "kopia.repository")
	if cacheDirectory != "" {
		b, err := ioutil.ReadFile(cachedFile)
		if err == nil {
			// read from cache.
			return parseFormatBlock(b)
		}
	}

	b, err := st.GetBlock(ctx, FormatBlockID, 0, -1)
	if err != nil {
		return nil, err
	}

	// block successfully read from storage.
	f, err := parseFormatBlock(b)
	if err != nil {
		return nil, err
	}

	if cacheDirectory != "" {
		if err := ioutil.WriteFile(cachedFile, b, 0600); err != nil {
			log.Warningf("warning: unable to write cache: %v", err)
		}
	}

	return f, nil
}
