package s3

// Options defines options for S3-based storage.
type Options struct {
	// BucketName is the name of the bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	Endpoint    string `json:"endpoint"`
	DoNotUseTLS bool   `json:"doNotUseTLS,omitempyy"`

	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey" kopia:"sensitive"`

	MaxUploadSpeedBytesPerSecond int `json:"maxUploadSpeedBytesPerSecond,omitempty"`

	MaxDownloadSpeedBytesPerSecond int `json:"maxDownloadSpeedBytesPerSecond,omitempty"`
}
