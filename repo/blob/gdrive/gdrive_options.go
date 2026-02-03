package gdrive

import (
	"encoding/json"

	"github.com/kopia/kopia/repo/blob/throttling"
	"golang.org/x/oauth2"
)

// OAuthConfig defines a serializable OAuth configuration.
type OAuthConfig struct {
	// AppId is Google Cloud Project number.
	AppId string `json:"appId"`
	// AppId is Google Cloud API key.
	ApiKey string `json:"apiKey"`
	// ClientId is Google Cloud OAuth2 Client ID.
	ClientId string `json:"clientId"`
	// ClientId is Google Cloud OAuth2 Client Secret.
	ClientSecret string `json:"clientSecret"`
}

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// FolderId is Google Drive's ID of a folder where data is stored.
	FolderID string `json:"folderID"`

	// ServiceAccountCredentialsFile specifies the name of the file with Drive credentials.
	ServiceAccountCredentialsFile string `json:"credentialsFile,omitempty"`

	// ServiceAccountCredentialJSON specifies the raw JSON credentials.
	ServiceAccountCredentialJSON json.RawMessage `json:"credentials,omitempty" kopia:"sensitive"`

	// OAuthConfig specifies OAuth configurations.
	OAuthConfig `json:"oauthConfig,omitempty"`

	// OAuthRefreshToken stores the long-term credentials for access.
	OAuthToken oauth2.Token `json:"oauthToken,omitempty"`

	throttling.Limits
}
