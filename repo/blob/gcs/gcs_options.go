package gcs

import (
	"encoding/json"
	"time"

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
	ServiceAccountCredentialJSON json.RawMessage `json:"credentials,omitempty" kopia:"sensitive"`

	// ReadOnly causes GCS connection to be opened with read-only scope to prevent accidental mutations.
	ReadOnly bool `json:"readOnly,omitempty"`

	throttling.Limits

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`
}
