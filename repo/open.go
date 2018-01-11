package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"
)

// Options provides configuration parameters for connection to a repository.
type Options struct {
	CredentialsCallback func() (auth.Credentials, error)    // Provides credentials required to open the repository if not persisted.
	TraceStorage        func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function

	DisableCache         bool // disable caching
	DisableListCache     bool // disable list caching
	ObjectManagerOptions object.ManagerOptions
}

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile string, options *Options) (rep *Repository, err error) {
	t0 := time.Now()
	log.Debug().Msgf("opening repository from %v", configFile)
	defer func() {
		if err == nil {
			log.Debug().Dur("duration_ms", time.Since(t0)).Msg("opened repository")
		} else {
			log.Error().Dur("duration_ms", time.Since(t0)).Msg("failed to open repository")
		}
	}()

	if options == nil {
		options = &Options{}
	}

	configFile, err = filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}

	log.Debug().Str("file", configFile).Msg("loading config")
	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	var creds auth.Credentials
	if len(lc.Connection.Key) > 0 {
		log.Debug().Msg("getting credentials from master key")
		creds, err = auth.MasterKey(lc.Connection.Key)
	} else {
		if options.CredentialsCallback == nil {
			return nil, errors.New("key not persisted and no credentials specified")
		}
		log.Debug().Msg("getting credentials using callback")
		creds, err = options.CredentialsCallback()
	}

	if err != nil {
		return nil, fmt.Errorf("invalid credentials: %v", err)
	}

	log.Debug().Str("connection", lc.Connection.ConnectionInfo.Type).Msg("opening storage")

	st, err := storage.NewStorage(ctx, lc.Connection.ConnectionInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %v", err)
	}

	caching := lc.Caching
	if options.DisableCache {
		caching = block.CachingOptions{}
	}
	if options.DisableListCache {
		caching.IgnoreListCache = true
	}

	r, err := connect(ctx, st, creds, options, caching)
	if err != nil {
		st.Close()
		return nil, err
	}

	r.ConfigFile = configFile

	return r, nil
}

// SetCachingConfig changes caching configuration for a given repository config file.
func SetCachingConfig(ctx context.Context, configFile string, opt block.CachingOptions) error {
	configFile, err := filepath.Abs(configFile)
	if err != nil {
		return err
	}

	log.Debug().Str("file", configFile).Msg("loading config")
	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	st, err := storage.NewStorage(ctx, lc.Connection.ConnectionInfo)
	if err != nil {
		return fmt.Errorf("cannot open storage: %v", err)
	}

	f, err := readAndCacheFormatBlock(st, "")
	if err != nil {
		return fmt.Errorf("can't read format block: %v", err)
	}

	if opt.MaxCacheSizeBytes > 0 {
		if opt.CacheDirectory == "" {
			// derive cache directory from config
			absConfig, err := filepath.Abs(configFile)
			if err != nil {
				return err
			}
			lc.Caching.CacheDirectory = filepath.Join(filepath.Dir(absConfig), fmt.Sprintf("cache-%x", f.UniqueID))
		} else {
			absCacheDir, err := filepath.Abs(opt.CacheDirectory)
			if err != nil {
				return err
			}

			lc.Caching.CacheDirectory = absCacheDir
		}
		lc.Caching.MaxCacheSizeBytes = opt.MaxCacheSizeBytes
		lc.Caching.MaxListCacheDurationSec = opt.MaxListCacheDurationSec

		log.Printf("Enabling cache directory '%v' with max size %v", lc.Caching.CacheDirectory, units.BytesStringBase2(lc.Caching.MaxCacheSizeBytes))
		os.MkdirAll(lc.Caching.CacheDirectory, 0700)
	} else {
		log.Printf("Disabling caching")
		lc.Caching = block.CachingOptions{}
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

func readAndCacheFormatBlock(st storage.Storage, cacheDirectory string) (*formatBlock, error) {
	cachedFile := filepath.Join(cacheDirectory, "kopia.repository")
	if cacheDirectory != "" {
		b, err := ioutil.ReadFile(cachedFile)
		if err == nil {
			// read from cache.
			return parseFormatBlock(b)
		}
	}

	b, err := st.GetBlock(formatBlockID, 0, -1)
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
			log.Printf("warning: unable to write cache: %v", err)
		}
	}

	return f, nil
}

func connect(ctx context.Context, st storage.Storage, creds auth.Credentials, options *Options, caching block.CachingOptions) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}
	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	log.Debug().Msg("reading encrypted format block")
	// Read cache block, potentially from cache.
	f, err := readAndCacheFormatBlock(st, caching.CacheDirectory)
	if err != nil {
		return nil, fmt.Errorf("unable to read format block: %v", err)
	}

	km, err := auth.NewKeyManager(creds, f.SecurityOptions)
	if err != nil {
		return nil, err
	}

	repoConfig, err := decryptFormatBytes(f, km)
	if err != nil {
		return nil, fmt.Errorf("unable to decrypt repository config: %v", err)
	}

	fo := repoConfig.FormattingOptions
	if fo.MaxPackSize == 0 {
		fo.MaxPackSize = repoConfig.MaxBlockSize
	}

	caching.HMACSecret = km.DeriveKey([]byte("local-cache-integrity"), 16)

	log.Debug().Msg("initializing block manager")
	bm, err := block.NewManager(st, fo, caching)
	if err != nil {
		return nil, fmt.Errorf("unable to open block manager: %v", err)
	}

	log.Debug().Msg("initializing object manager")
	om, err := object.NewObjectManager(bm, *repoConfig, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	log.Debug().Msg("initializing manifest manager")
	manifests, err := manifest.NewManager(bm)
	if err != nil {
		return nil, fmt.Errorf("unable to open manifests: %v", err)
	}

	return &Repository{
		Blocks:         bm,
		Objects:        om,
		Storage:        st,
		KeyManager:     km,
		Manifests:      manifests,
		Security:       f.SecurityOptions,
		CacheDirectory: caching.CacheDirectory,
	}, nil
}
