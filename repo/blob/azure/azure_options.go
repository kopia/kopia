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

	// ClientCertificate are used for creating ClientCertificateCredentials
	ClientCertificate string `json:"clientCertificate,omitempty" kopia:"sensitive"`

	// AzureFederatedTokenFile is the path to a file containing an Azure Federated Token.
	AzureFederatedTokenFile string `json:"azureFederatedTokenFile,omitempty"`

	StorageDomain string `json:"storageDomain,omitempty"`

	// DoNotUseTLS connects to Azure storage over HTTP instead of HTTPS
	DoNotUseTLS bool `json:"doNotUseTLS,omitempty"`

	throttling.Limits

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`

	// UseAzureCLICredential forces Kopia to try Azure CLI cached credential (reads `az login` cache).
	// If not set, Kopia will use DefaultAzureCredential (env -> managed identity -> shared cache -> CLI -> ...).
	// English comment: prefer Azure CLI cached token (useful for `az login` and `az login --identity` flows).
	UseAzureCLICredential bool `json:"useAzureCliCredential,omitempty"`
}
