package gdrive

import (
	"encoding/json"

	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// FolderId is Google Drive's ID of a folder where data is stored.
	FolderID string `json:"folderID"`

	// ServiceAccountCredentialsFile specifies the name of the file with Drive credentials.
	ServiceAccountCredentialsFile string `json:"credentialsFile,omitempty"`

	// ServiceAccountCredentialJSON specifies the raw JSON credentials.
	ServiceAccountCredentialJSON json.RawMessage `json:"credentials,omitempty" kopia:"sensitive"`

	// ReadOnly causes GCS connection to be opened with read-only scope to prevent accidental mutations.
	ReadOnly bool `json:"readOnly,omitempty"`

	throttling.Limits
}
