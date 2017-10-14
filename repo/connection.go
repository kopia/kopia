package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

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

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	PersistCredentials bool
	CacheDirectory     string
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, creds auth.Credentials, opt ConnectOptions) error {
	r, err := connect(ctx, st, creds, nil)
	if err != nil {
		return err
	}

	cfg, err := r.Metadata.connectionConfiguration()
	if err != nil {
		return err
	}

	var lc config.LocalConfig
	lc.Connection = cfg

	if !opt.PersistCredentials {
		lc.Connection.Key = nil
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(configFile), 0700); err != nil {
		return err
	}

	return ioutil.WriteFile(configFile, d, 0600)
}

func connect(ctx context.Context, st storage.Storage, creds auth.Credentials, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}
	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	mm, err := newMetadataManager(st, creds)
	if err != nil {
		return nil, fmt.Errorf("unable to open metadata manager: %v", err)
	}

	sf := block.FormatterFactories[mm.repoConfig.Format.BlockFormat]
	if sf == nil {
		return nil, fmt.Errorf("unsupported block format: %v", mm.repoConfig.Format.BlockFormat)
	}

	formatter, err := sf(mm.repoConfig.Format)
	if err != nil {
		return nil, err
	}

	bm := block.NewManager(st, mm.repoConfig.Format.MaxPackedContentLength, mm.repoConfig.Format.MaxBlockSize, formatter)

	om, err := object.NewObjectManager(bm, mm.repoConfig.Format, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	return &Repository{
		Blocks:   bm,
		Objects:  om,
		Metadata: mm,
		Storage:  st,
	}, nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	_, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	return os.Remove(configFile)
}
