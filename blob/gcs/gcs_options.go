package gcs

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// IgnoreDefaultCredentials disables the use of credentials managed by Google Cloud SDK (gcloud).
	IgnoreDefaultCredentials bool `json:"ignoreDefaultCredentials"`
}
