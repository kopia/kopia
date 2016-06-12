package vault

import (
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/repo"
)

// Format describes the format of a Vault.
// Contents of this structure are serialized in plain text in the Vault storage.
type Format struct {
	Version    string `json:"version"`
	UniqueID   []byte `json:"uniqueID"`
	Encryption string `json:"encryption"`
	Checksum   string `json:"checksum"`
}

type repositoryConfig struct {
	Connection blob.ConnectionInfo `json:"connection"`
	Format     *repo.Format        `json:"format"`
}
