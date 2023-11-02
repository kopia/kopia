package storj

import (
	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options Storj-backed storage.
type Options struct {
	// BucketName is the name of the Storj bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// Access Grant is the access grant generated on the satellite. Use 'uplink access export' to export a
	// valiad access grant string that needs to be set here.
	AccessGrant string `json:"accessgrant"`

	throttling.Limits
}
