package content

// FormattingOptions describes the rules for formatting contents in repository.
type FormattingOptions struct {
	Version     int    `json:"version,omitempty"`     // version number, must be "1"
	Hash        string `json:"hash,omitempty"`        // identifier of the hash algorithm used
	Encryption  string `json:"encryption,omitempty"`  // identifier of the encryption algorithm used
	HMACSecret  []byte `json:"secret,omitempty"`      // HMAC secret used to generate encryption keys
	MasterKey   []byte `json:"masterKey,omitempty"`   // master encryption key (SIV-mode encryption only)
	MaxPackSize int    `json:"maxPackSize,omitempty"` // maximum size of a pack object
}

// GetEncryptionAlgorithm implements encryption.Parameters.
func (f *FormattingOptions) GetEncryptionAlgorithm() string {
	return f.Encryption
}

// GetMasterKey implements encryption.Parameters.
func (f *FormattingOptions) GetMasterKey() []byte {
	return f.MasterKey
}

// GetHashFunction implements hashing.Parameters.
func (f *FormattingOptions) GetHashFunction() string {
	return f.Hash
}

// GetHmacSecret implements hashing.Parameters.
func (f *FormattingOptions) GetHmacSecret() []byte {
	return f.HMACSecret
}
