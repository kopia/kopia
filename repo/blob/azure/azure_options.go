package azure

import (
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for Azure blob storage storage.
type Options struct {
	// Container is the name of the azure storage container where data is stored.
	Container string `json:"container"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// Azure Storage account name and key
	StorageAccount string `json:"storageAccount"`
	StorageKey     string `json:"storageKey"     kopia:"sensitive"`

	// Alternatively provide SAS Token
	SASToken string `json:"sasToken" kopia:"sensitive"`

	StorageDomain string `json:"storageDomain,omitempty"`

	throttling.Limits
}
