package gcs

import "golang.org/x/oauth2"

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// TokenCacheFile is the name of the file that will persist the OAuth2 token.
	// If not specified, the token will be persisted in Options.
	TokenCacheFile string `json:"tokenCacheFile,omitempty"`

	// Token is the OAuth2 token (when TokenCacheFile is empty)
	Token *oauth2.Token `json:"token,omitempty"`

	// ReadOnly causes the storage to be configured without write permissions, to prevent accidental
	// modifications to the data.
	ReadOnly bool `json:"readonly"`

	// IgnoreDefaultCredentials disables the use of credentials managed by Google Cloud SDK (gcloud).
	IgnoreDefaultCredentials bool `json:"ignoreDefaultCredentials"`
}
