package repo

import (
	"github.com/kopia/kopia/auth"
)

// VaultFormat describes the format of a Vault.
// Contents of this structure are serialized in plain text in the Vault storage.
type VaultFormat struct {
	auth.Options
	Version             string `json:"version"`
	EncryptionAlgorithm string `json:"encryption"`
}
