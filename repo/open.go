package repo

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	"github.com/kopia/kopia/manifest"

	"github.com/kopia/kopia/object"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/logging"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/storage/filesystem"
	_ "github.com/kopia/kopia/storage/gcs"
)

// Options provides configuration parameters for connection to a repository.
type Options struct {
	CredentialsCallback func() (auth.Credentials, error)    // Provides credentials required to open the repository if not persisted.
	TraceStorage        func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function

	ObjectManagerOptions object.ManagerOptions
}

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile string, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}

	configFile, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}

	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	var creds auth.Credentials
	if len(lc.Connection.Key) > 0 {
		creds, err = auth.MasterKey(lc.Connection.Key)
	} else {
		if options.CredentialsCallback == nil {
			return nil, errors.New("key not persisted and no credentials specified")
		}
		creds, err = options.CredentialsCallback()
	}

	if err != nil {
		return nil, fmt.Errorf("invalid credentials: %v", err)
	}

	st, err := storage.NewStorage(ctx, lc.Connection.ConnectionInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %v", err)
	}

	caching := lc.Caching
	r, err := connect(ctx, st, creds, options, caching)
	if err != nil {
		st.Close()
		return nil, err
	}

	r.ConfigFile = configFile

	return r, nil
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

	bm, err := block.NewManager(st, fo, caching)
	if err != nil {
		return nil, fmt.Errorf("unable to open block manager: %v", err)
	}

	om, err := object.NewObjectManager(bm, *repoConfig, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

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
