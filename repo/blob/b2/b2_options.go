package b2

import "github.com/kopia/kopia/repo/blob/throttling"

// Options defines options for B2-based storage.
type Options struct {
	// BucketName is the name of the bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	KeyID string `json:"keyID"`
	Key   string `json:"key"   kopia:"sensitive"`

	throttling.Limits
}
