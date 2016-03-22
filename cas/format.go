package cas

// Format describes the format of object data.
type Format struct {
	Algorithm         string `json:"algorithm"`
	Secret            []byte `json:"secret,omitempty"`
	MaxInlineBlobSize int    `json:"maxInlineBlobSize"`
	MaxBlobSize       int    `json:"maxBlobSize"`
}
