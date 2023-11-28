package azure

import (
	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for Azure blob storage storage.
type Options struct {
	// Container is the name of the azure storage container where data is stored.
	Container string `json:"container"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// Storage account name
	StorageAccount string `json:"storageAccount"`

	// Storage account access key
	StorageKey *secrets.Secret `json:"storageKey"     kopia:"sensitive"`

	// Alternatively provide SAS Token
	SASToken *secrets.Secret `json:"sasToken" kopia:"sensitive"`

	// the tenant-ID/client-ID/client-Secret of the service principal
	TenantID     string
	ClientID     string
	ClientSecret string

	StorageDomain string `json:"storageDomain,omitempty"`

	throttling.Limits
}
