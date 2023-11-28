package gdrive

import (
	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// FolderId is Google Drive's ID of a folder where data is stored.
	FolderID string `json:"folderID"`

	// ServiceAccountCredentialsFile specifies the name of the file with Drive credentials.
	ServiceAccountCredentialsFile string `json:"credentialsFile,omitempty"`

	// ServiceAccountCredentialJSON specifies the raw JSON credentials.
	ServiceAccountCredentialJSON *secrets.Secret `json:"credentials,omitempty" kopia:"sensitive"`

	// ReadOnly causes GCS connection to be opened with read-only scope to prevent accidental mutations.
	ReadOnly bool `json:"readOnly,omitempty"`

	throttling.Limits
}
