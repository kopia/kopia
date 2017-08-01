package config

import (
	"github.com/kopia/kopia/auth"
)

// MetadataFormat describes the format of metadata items in repository.
// Contents of this structure are serialized in plain text in the storage.
type MetadataFormat struct {
	auth.SecurityOptions
	Version             string `json:"version"`
	EncryptionAlgorithm string `json:"encryption"`
}
