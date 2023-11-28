package gcs

import (
	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// ServiceAccountCredentialsFile specifies the name of the file with GCS credentials.
	ServiceAccountCredentialsFile string `json:"credentialsFile,omitempty"`

	// ServiceAccountCredentialJSON specifies the raw JSON credentials.
	ServiceAccountCredentialJSON *secrets.Secret `json:"credentials,omitempty" kopia:"sensitive"`

	// ReadOnly causes GCS connection to be opened with read-only scope to prevent accidental mutations.
	ReadOnly bool `json:"readOnly,omitempty"`

	throttling.Limits
}
