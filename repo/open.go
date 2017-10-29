package repo

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/metadata"
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

	r, err := connect(ctx, st, creds, options)
	if err != nil {
		st.Close()
		return nil, err
	}

	r.ConfigFile = configFile
	r.CacheDirectory = applyDefaultString(lc.CacheDirectory, filepath.Join(filepath.Dir(configFile), "cache"))

	return r, nil
}

func connect(ctx context.Context, st storage.Storage, creds auth.Credentials, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}
	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	f, err := readFormaBlock(st)
	if err != nil {
		return nil, err
	}

	km, err := auth.NewKeyManager(creds, f.SecurityOptions)
	if err != nil {
		return nil, err
	}

	mm, err := metadata.NewManager(st, f.Format, km)
	if err != nil {
		return nil, fmt.Errorf("unable to open metadata manager: %v", err)
	}

	erc, err := readEncryptedConfig(mm)
	if err != nil {
		return nil, fmt.Errorf("unable to read encrypted config: %v", err)
	}

	repoConfig := erc.Format

	sf := block.FormatterFactories[repoConfig.BlockFormat]
	if sf == nil {
		return nil, fmt.Errorf("unsupported block format: %v", repoConfig.BlockFormat)
	}

	formatter, err := sf(repoConfig.FormattingOptions)
	if err != nil {
		return nil, err
	}

	bm := block.NewManager(st, repoConfig.MaxPackedContentLength, repoConfig.MaxBlockSize, formatter)

	om, err := object.NewObjectManager(bm, repoConfig, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	return &Repository{
		Blocks:     bm,
		Objects:    om,
		Metadata:   mm,
		Storage:    st,
		KeyManager: km,
		Security:   f.SecurityOptions,
	}, nil
}
