package repo

import (
	"github.com/kopia/kopia/auth"
)

// ConnectOptions specifies the behavior of Connection.
type ConnectOptions struct {
	CredentialsCallback func() (auth.Credentials, error) // credentials required to open the vault, unless persisted

	TraceStorage      func(f string, args ...interface{})
	RepositoryOptions []RepositoryOption

	MaxDownloadSpeed int
	MaxUploadSpeed   int
}
