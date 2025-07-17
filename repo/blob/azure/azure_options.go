package azure

import (
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for Azure blob storage storage.
type Options struct {
	// Container is the name of the azure storage container where data is stored.
	Container string `json:"container"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// Storage account name
	StorageAccount string `json:"storageAccount,omitempty"`

	// Storage account access key
	StorageKey string `json:"storageKey,omitempty" kopia:"sensitive"`

	// Alternatively provide SAS Token
	SASToken string `json:"sasToken,omitempty" kopia:"sensitive"`

	// the tenant-ID/client-ID/client-Secret of the service principal
	TenantID     string `json:",omitempty"`
	ClientID     string `json:",omitempty"`
	ClientSecret string `json:",omitempty" kopia:"sensitive"`

	// ClientCert are used for creating ClientCertificateCredentials
	ClientCert string `json:"clientCertificate,omitempty" kopia:"sensitive"`

	StorageDomain string `json:"storageDomain,omitempty"`

	throttling.Limits

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`
}
