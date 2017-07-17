package client

import (
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"
)

// Options specifies the behavior of Connection.
type Options struct {
	CredentialsCallback func() (vault.Credentials, error) // credentials required to open the vault, unless persisted

	TraceStorage      func(f string, args ...interface{})
	RepositoryOptions []repo.RepositoryOption

	MaxDownloadSpeed int
	MaxUploadSpeed   int
}
