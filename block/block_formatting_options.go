package block

// FormattingOptions describes the rules for formatting blocks in repository.
type FormattingOptions struct {
	Version     int    `json:"version,omitempty"`      // version number, must be "1"
	BlockFormat string `json:"objectFormat,omitempty"` // identifier of the block format
	HMACSecret  []byte `json:"secret,omitempty"`       // HMAC secret used to generate encryption keys
	MasterKey   []byte `json:"masterKey,omitempty"`    // master encryption key (SIV-mode encryption only)
	MaxPackSize int    `json:"maxPackSize,omitempty"`  // maximum size of a pack object
}
