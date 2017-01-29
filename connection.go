package kopia

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/caching"
	"github.com/kopia/kopia/blob/logging"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/blob/filesystem"
	_ "github.com/kopia/kopia/blob/gcs"
	"github.com/kopia/kopia/snapshot"
)

// Connection represents open connection to Vault and Repository.
type Connection struct {
	Vault           *vault.Vault
	Repository      *repo.Repository
	SnapshotManager *snapshot.Manager
}

// ConnectionOptions specifies the behavior of Connection.
type ConnectionOptions struct {
	CredentialsCallback func() (vault.Credentials, error) // credentials required to open the vault, unless persisted

	TraceStorage      func(f string, args ...interface{})
	RepositoryOptions []repo.RepositoryOption

	MaxDownloadSpeed int
	MaxUploadSpeed   int
}

// Close closes the underlying Vault and Repository.
func (c *Connection) Close() error {
	if c.Vault != nil {
		c.Vault.Close()
		c.Vault = nil
	}

	if c.Repository != nil {
		c.Repository.Close()
		c.Repository = nil
	}

	return nil
}

// Open connects to the Vault and Repository specified in the specified configuration file.
func Open(ctx context.Context, configFile string, options *ConnectionOptions) (*Connection, error) {
	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	var creds vault.Credentials
	if len(lc.VaultConnection.Key) > 0 {
		creds, err = vault.MasterKey(lc.VaultConnection.Key)
	} else {
		if options.CredentialsCallback == nil {
			return nil, errors.New("vault key not persisted and no credentials specified")
		}
		creds, err = options.CredentialsCallback()
	}

	if err != nil {
		return nil, fmt.Errorf("invalid vault credentials: %v", err)
	}

	rawVaultStorage, err := newStorageWithOptions(ctx, lc.VaultConnection.ConnectionInfo, options)
	if err != nil {
		return nil, fmt.Errorf("cannot open vault storage: %v", err)
	}

	vaultStorage := rawVaultStorage

	if options.TraceStorage != nil {
		vaultStorage = logging.NewWrapper(vaultStorage, logging.Prefix("[VAULT] "), logging.Output(options.TraceStorage))
	}

	var conn Connection
	conn.Vault, err = vault.Open(vaultStorage, creds)
	if err != nil {
		rawVaultStorage.Close()
		return nil, fmt.Errorf("unable to open vault: %v", err)
	}

	var repositoryStorage blob.Storage

	if lc.RepoConnection == nil {
		repositoryStorage = rawVaultStorage
	} else {
		repositoryStorage, err = newStorageWithOptions(ctx, *lc.RepoConnection, options)
		if err != nil {
			vaultStorage.Close()
			return nil, err
		}
	}

	if options.TraceStorage != nil {
		repositoryStorage = logging.NewWrapper(repositoryStorage, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	if lc.Caching != nil {
		rs, err := caching.NewWrapper(ctx, repositoryStorage, lc.Caching)
		if err != nil {
			vaultStorage.Close()
			repositoryStorage.Close()
			return nil, err
		}
		repositoryStorage = rs
		if options.TraceStorage != nil {
			repositoryStorage = logging.NewWrapper(repositoryStorage, logging.Prefix("[CACHE] "), logging.Output(options.TraceStorage))
		}
	}

	conn.Repository, err = repo.New(repositoryStorage, conn.Vault.RepoConfig.Format)
	if err != nil {
		vaultStorage.Close()
		repositoryStorage.Close()
		return nil, err
	}
	conn.SnapshotManager = snapshot.NewManager(conn.Vault)

	return &conn, nil
}

func newStorageWithOptions(ctx context.Context, cfg blob.ConnectionInfo, options *ConnectionOptions) (blob.Storage, error) {
	s, err := blob.NewStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if options.MaxUploadSpeed > 0 || options.MaxDownloadSpeed > 0 {
		t, ok := s.(blob.Throttler)
		if ok {
			t.SetThrottle(options.MaxDownloadSpeed, options.MaxUploadSpeed)
		} else {
			log.Printf("Throttling not supported for '%v'.", cfg.Type)
		}
	}

	return s, nil
}
