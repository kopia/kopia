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

func readMetadataFormat(st storage.Storage) (*config.MetadataFormat, error) {
	f := &config.MetadataFormat{}

	b, err := st.GetBlock("VLTformat", 0, -1)
	if err != nil {
		return nil, fmt.Errorf("unable to read format block: %v", err)
	}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("invalid format block: %v", err)
	}
	return f, err
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, creds auth.Credentials, opt ConnectOptions) error {
	cip, ok := st.(storage.ConnectionInfoProvider)
	if !ok {
		return errors.New("repository does not support persisting configuration")
	}

	f, err := readMetadataFormat(st)
	if err != nil {
		return err
	}

	masterKey, err := creds.GetMasterKey(f.SecurityOptions)
	if err != nil {
		return fmt.Errorf("can't get master key: %v", err)
	}

	cfg := &config.RepositoryConnectionInfo{
		ConnectionInfo: cip.ConnectionInfo(),
		Key:            masterKey,
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

	if err := ioutil.WriteFile(configFile, d, 0600); err != nil {
		return nil
	}

	// now verify that the repository can be opened with the provided config file.
	r, err := Open(ctx, configFile, &Options{
		CredentialsCallback: func() (auth.Credentials, error) {
			return creds, nil
		},
	})
	if err != nil {
		return err
	}

	r.Close()
	return nil
}

func connect(ctx context.Context, st storage.Storage, creds auth.Credentials, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}
	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	f, err := readMetadataFormat(st)
	if err != nil {
		return nil, err
	}

	km, err := auth.NewKeyManager(creds, f.SecurityOptions)
	if err != nil {
		return nil, err
	}

	mm, err := newMetadataManager(st, f, km)
	if err != nil {
		return nil, fmt.Errorf("unable to open metadata manager: %v", err)
	}

	var erc config.EncryptedRepositoryConfig

	if err := mm.getJSON("repo", &erc); err != nil {
		return nil, fmt.Errorf("unable to read repository configuration: %v", err)
	}

	repoConfig := erc.Format

	sf := block.FormatterFactories[repoConfig.BlockFormat]
	if sf == nil {
		return nil, fmt.Errorf("unsupported block format: %v", repoConfig.BlockFormat)
	}

	formatter, err := sf(repoConfig)
	if err != nil {
		return nil, err
	}

	bm := block.NewManager(st, repoConfig.MaxPackedContentLength, repoConfig.MaxBlockSize, formatter)

	om, err := object.NewObjectManager(bm, repoConfig, options.ObjectManagerOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	return &Repository{
		Blocks:         bm,
		Objects:        om,
		Metadata:       mm,
		Storage:        st,
		KeyManager:     km,
		metadataFormat: f,
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
