package cas

// Format describes the format of object data.
type Format struct {
	Version           string `json:"version"`
	Hash              string `json:"hash"`
	Encryption        string `json:"encryption"`
	Secret            []byte `json:"secret,omitempty"`
	MaxInlineBlobSize int    `json:"maxInlineBlobSize"`
	MaxBlobSize       int    `json:"maxBlobSize"`
}
