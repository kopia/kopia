package block

// FormattingOptions describes the rules for formatting blocks in repository.
type FormattingOptions struct {
	Version     int    `json:"version,omitempty"`     // version number, must be "1"
	Hash        string `json:"hash,omitempty"`        // identifier of the hash algorithm used
	Encryption  string `json:"encryption,omitempty"`  // identifier of the encryption algorithm used
	HMACSecret  []byte `json:"secret,omitempty"`      // HMAC secret used to generate encryption keys
	MasterKey   []byte `json:"masterKey,omitempty"`   // master encryption key (SIV-mode encryption only)
	MaxPackSize int    `json:"maxPackSize,omitempty"` // maximum size of a pack object
}
