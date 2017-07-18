package client

import (
	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/repo"
)

// Options specifies the behavior of Connection.
type Options struct {
	CredentialsCallback func() (auth.Credentials, error) // credentials required to open the vault, unless persisted

	TraceStorage      func(f string, args ...interface{})
	RepositoryOptions []repo.RepositoryOption

	MaxDownloadSpeed int
	MaxUploadSpeed   int
}
