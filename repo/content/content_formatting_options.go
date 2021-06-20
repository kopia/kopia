package content

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
)

const (
	minValidPackSize = 10 << 20
	maxValidPackSize = 120 << 20
)

// FormattingOptions describes the rules for formatting contents in repository.
type FormattingOptions struct {
	Version    int    `json:"version,omitempty"`    // version number, must be "1"
	Hash       string `json:"hash,omitempty"`       // identifier of the hash algorithm used
	Encryption string `json:"encryption,omitempty"` // identifier of the encryption algorithm used
	HMACSecret []byte `json:"secret,omitempty"`     // HMAC secret used to generate encryption keys
	MasterKey  []byte `json:"masterKey,omitempty"`  // master encryption key (SIV-mode encryption only)
	MutableParameters
}

// MutableParameters represents parameters of the content manager that can be mutated after the repository
// is created.
type MutableParameters struct {
	MaxPackSize  int `json:"maxPackSize,omitempty"`  // maximum size of a pack object
	IndexVersion int `json:"indexVersion,omitempty"` // force particular index format version (1,2,..)
}

// Validate validates the parameters.
func (v *MutableParameters) Validate() error {
	if v.MaxPackSize < minValidPackSize {
		return errors.Errorf("max pack size too small, must be >= %v", units.BytesStringBase2(minValidPackSize))
	}

	if v.MaxPackSize > maxValidPackSize {
		return errors.Errorf("max pack size too big, must be <= %v", units.BytesStringBase2(maxValidPackSize))
	}

	if v.IndexVersion < 0 || v.IndexVersion > v2IndexVersion {
		return errors.Errorf("invalid index version, supported versions are 1 & 2")
	}

	return nil
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
