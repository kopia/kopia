package s3

import (
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

// Options defines options for S3-based storage.
type Options struct {
	// BucketName is the name of the bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	Endpoint       string `json:"endpoint"`
	DoNotUseTLS    bool   `json:"doNotUseTLS,omitempty"`
	DoNotVerifyTLS bool   `json:"doNotVerifyTLS,omitempty"`
	RootCA         []byte `json:"rootCA,omitempty"`

	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey" kopia:"sensitive"`
	SessionToken    string `json:"sessionToken"    kopia:"sensitive"`

	// Region is an optional region to pass in authorization header.
	Region string `json:"region,omitempty"`

	throttling.Limits

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`
}
