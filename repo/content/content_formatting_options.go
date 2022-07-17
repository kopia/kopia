package content

import (
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

const (
	minValidPackSize = 10 << 20
	maxValidPackSize = 120 << 20
)

// FormatVersion denotes content format version.
type FormatVersion int

// Supported format versions.
const (
	FormatVersion1 FormatVersion = 1
	FormatVersion2 FormatVersion = 2 // new in v0.9

	MaxFormatVersion = FormatVersion2
)

// FormattingOptions describes the rules for formatting contents in repository.
type FormattingOptions struct {
	Hash       string `json:"hash,omitempty"`                        // identifier of the hash algorithm used
	Encryption string `json:"encryption,omitempty"`                  // identifier of the encryption algorithm used
	HMACSecret []byte `json:"secret,omitempty" kopia:"sensitive"`    // HMAC secret used to generate encryption keys
	MasterKey  []byte `json:"masterKey,omitempty" kopia:"sensitive"` // master encryption key (SIV-mode encryption only)
	MutableParameters

	EnablePasswordChange bool `json:"enablePasswordChange"` // disables replication of kopia.repository blob in packs
}

// ResolveFormatVersion applies format options parameters based on the format version.
func (f *FormattingOptions) ResolveFormatVersion() error {
	switch f.Version {
	case FormatVersion2:
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

// MutableParameters represents parameters of the content manager that can be mutated after the repository
// is created.
type MutableParameters struct {
	Version         FormatVersion    `json:"version,omitempty"`         // version number, must be "1" or "2"
	MaxPackSize     int              `json:"maxPackSize,omitempty"`     // maximum size of a pack object
	IndexVersion    int              `json:"indexVersion,omitempty"`    // force particular index format version (1,2,..)
	EpochParameters epoch.Parameters `json:"epochParameters,omitempty"` // epoch manager parameters
}

// Validate validates the parameters.
func (v *MutableParameters) Validate() error {
	if v.MaxPackSize < minValidPackSize {
		return errors.Errorf("max pack size too small, must be >= %v", units.BytesStringBase2(minValidPackSize))
	}

	if v.MaxPackSize > maxValidPackSize {
		return errors.Errorf("max pack size too big, must be <= %v", units.BytesStringBase2(maxValidPackSize))
	}

	if v.IndexVersion < 0 || v.IndexVersion > index.Version2 {
		return errors.Errorf("invalid index version, supported versions are 1 & 2")
	}

	if err := v.EpochParameters.Validate(); err != nil {
		return errors.Wrap(err, "invalid epoch parameters")
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

// BlobCfgBlob is the content for `kopia.blobcfg` blob which contains the blob
// management configuration options.
type BlobCfgBlob struct {
	RetentionMode   blob.RetentionMode `json:"retentionMode,omitempty"`
	RetentionPeriod time.Duration      `json:"retentionPeriod,omitempty"`
}

// IsRetentionEnabled returns true if retention is enabled on the blob-config
// object.
func (r *BlobCfgBlob) IsRetentionEnabled() bool {
	return r.RetentionMode != "" && r.RetentionPeriod != 0
}

// Validate validates the blob config parameters.
func (r *BlobCfgBlob) Validate() error {
	if (r.RetentionMode == "") != (r.RetentionPeriod == 0) {
		return errors.Errorf("both retention mode and period must be provided when setting blob retention properties")
	}

	if r.RetentionPeriod != 0 && r.RetentionPeriod < 24*time.Hour {
		return errors.Errorf("invalid retention-period, the minimum required is 1-day and there is no maximum limit")
	}

	return nil
}
