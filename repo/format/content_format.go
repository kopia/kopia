package format

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/content/index"
)

// ContentFormat describes the rules for formatting contents in repository.
type ContentFormat struct {
	Hash               string `json:"hash,omitempty"`                        // identifier of the hash algorithm used
	Encryption         string `json:"encryption,omitempty"`                  // identifier of the encryption algorithm used
	ECC                string `json:"ecc,omitempty"`                         // identifier of the ecc algorithm used
	ECCOverheadPercent int    `json:"eccOverheadPercent,omitempty"`          // space overhead for ecc
	HMACSecret         []byte `json:"secret,omitempty" kopia:"sensitive"`    // HMAC secret used to generate encryption keys
	MasterKey          []byte `json:"masterKey,omitempty" kopia:"sensitive"` // master encryption key (SIV-mode encryption only)
	MutableParameters

	EnablePasswordChange bool `json:"enablePasswordChange"` // disables replication of kopia.repository blob in packs
}

// ResolveFormatVersion applies format options parameters based on the format version.
func (f *ContentFormat) ResolveFormatVersion() error {
	switch f.Version {
	case FormatVersion2, FormatVersion3:
		f.EnablePasswordChange = true
		f.IndexVersion = index.Version2
		f.EpochParameters = epoch.DefaultParameters()

		return nil

	case FormatVersion1:
		f.EnablePasswordChange = false
		f.IndexVersion = index.Version1
		f.EpochParameters = epoch.Parameters{}

		return nil

	default:
		return errors.Errorf("Unsupported format version: %v", f.Version)
	}
}

// GetMutableParameters implements FormattingOptionsProvider.
func (f *ContentFormat) GetMutableParameters(ctx context.Context) (MutableParameters, error) {
	return f.MutableParameters, nil
}

// GetCachedMutableParameters implements FormattingOptionsProvider.
func (f *ContentFormat) GetCachedMutableParameters() MutableParameters {
	return f.MutableParameters
}

// SupportsPasswordChange implements FormattingOptionsProvider.
func (f *ContentFormat) SupportsPasswordChange() bool {
	return f.EnablePasswordChange
}

// MutableParameters represents parameters of the content manager that can be mutated after the repository
// is created.
type MutableParameters struct {
	Version         Version          `json:"version,omitempty"`         // version number, must be "1", "2" or "3"
	MaxPackSize     int              `json:"maxPackSize,omitempty"`     // maximum size of a pack object
	IndexVersion    int              `json:"indexVersion,omitempty"`    // force particular index format version (1,2,..)
	EpochParameters epoch.Parameters `json:"epochParameters,omitempty"` // epoch manager parameters
}

// Validate validates the parameters.
func (v *MutableParameters) Validate() error {
	if v.MaxPackSize < minValidPackSize {
		return errors.Errorf("max pack size too small, must be >= %v", units.BytesString(minValidPackSize))
	}

	if v.MaxPackSize > maxValidPackSize {
		return errors.Errorf("max pack size too big, must be <= %v", units.BytesString(maxValidPackSize))
	}

	if v.IndexVersion < 0 || v.IndexVersion > index.Version2 {
		return errors.New("invalid index version, supported versions are 1 & 2")
	}

	if err := v.EpochParameters.Validate(); err != nil {
		return errors.Wrap(err, "invalid epoch parameters")
	}

	return nil
}

// GetEncryptionAlgorithm implements encryption.Parameters.
func (f *ContentFormat) GetEncryptionAlgorithm() string {
	return f.Encryption
}

// GetMasterKey implements encryption.Parameters.
func (f *ContentFormat) GetMasterKey() []byte {
	return f.MasterKey
}

// GetECCAlgorithm implements ecc.Parameters.
func (f *ContentFormat) GetECCAlgorithm() string {
	return f.ECC
}

// GetECCOverheadPercent implements ecc.Parameters.
func (f *ContentFormat) GetECCOverheadPercent() int {
	return f.ECCOverheadPercent
}

// GetHashFunction implements hashing.Parameters.
func (f *ContentFormat) GetHashFunction() string {
	return f.Hash
}

// GetHmacSecret implements hashing.Parameters.
func (f *ContentFormat) GetHmacSecret() []byte {
	return f.HMACSecret
}
