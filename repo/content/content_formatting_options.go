package content

import (
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
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
	FormatVersion3 FormatVersion = 3 // new in v0.11

	MaxFormatVersion = FormatVersion3
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
func (f *FormattingOptions) GetMutableParameters() MutableParameters {
	return f.MutableParameters
}

// SupportsPasswordChange implements FormattingOptionsProvider.
func (f *FormattingOptions) SupportsPasswordChange() bool {
	return f.EnablePasswordChange
}

// MutableParameters represents parameters of the content manager that can be mutated after the repository
// is created.
type MutableParameters struct {
	Version         FormatVersion    `json:"version,omitempty"`         // version number, must be "1", "2" or "3"
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

// FormattingOptionsProvider provides current formatting options. The options returned
// should not be cached for more than a few seconds as they are subject to change.
type FormattingOptionsProvider interface {
	epoch.ParametersProvider

	MaxIndexBlobSize() int64
	WriteIndexVersion() int
	IndexShardSize() int

	encryption.Parameters
	hashing.Parameters

	HashFunc() hashing.HashFunc
	Encryptor() encryption.Encryptor

	GetMutableParameters() MutableParameters
	GetMasterKey() []byte
	SupportsPasswordChange() bool
	FormatVersion() FormatVersion
	MaxPackBlobSize() int
	RepositoryFormatBytes() []byte
	Struct() FormattingOptions
}

type formattingOptionsProvider struct {
	*FormattingOptions

	h                   hashing.HashFunc
	e                   encryption.Encryptor
	actualFormatVersion FormatVersion
	actualIndexVersion  int
	formatBytes         []byte
}

func (f *formattingOptionsProvider) FormatVersion() FormatVersion {
	return f.Version
}

// whether epoch manager is enabled, must be true.
func (f *formattingOptionsProvider) GetEpochManagerEnabled() bool {
	return f.EpochParameters.Enabled
}

// how frequently each client will list blobs to determine the current epoch.
func (f *formattingOptionsProvider) GetEpochRefreshFrequency() time.Duration {
	return f.EpochParameters.EpochRefreshFrequency
}

// number of epochs between full checkpoints.
func (f *formattingOptionsProvider) GetEpochFullCheckpointFrequency() int {
	return f.EpochParameters.FullCheckpointFrequency
}

// GetEpochCleanupSafetyMargin returns safety margin to prevent uncompacted blobs from being deleted if the corresponding compacted blob age is less than this.
func (f *formattingOptionsProvider) GetEpochCleanupSafetyMargin() time.Duration {
	return f.EpochParameters.CleanupSafetyMargin
}

// GetMinEpochDuration returns the minimum duration of an epoch.
func (f *formattingOptionsProvider) GetMinEpochDuration() time.Duration {
	return f.EpochParameters.MinEpochDuration
}

// GetEpochAdvanceOnCountThreshold returns the number of files above which epoch should be advanced.
func (f *formattingOptionsProvider) GetEpochAdvanceOnCountThreshold() int {
	return f.EpochParameters.EpochAdvanceOnCountThreshold
}

// GetEpochAdvanceOnTotalSizeBytesThreshold returns the total size of files above which the epoch should be advanced.
func (f *formattingOptionsProvider) GetEpochAdvanceOnTotalSizeBytesThreshold() int64 {
	return f.EpochParameters.EpochAdvanceOnTotalSizeBytesThreshold
}

// GetEpochDeleteParallelism returns the number of blobs to delete in parallel during cleanup.
func (f *formattingOptionsProvider) GetEpochDeleteParallelism() int {
	return f.EpochParameters.DeleteParallelism
}

func (f *formattingOptionsProvider) Struct() FormattingOptions {
	return *f.FormattingOptions
}

// NewFormattingOptionsProvider validates the provided formatting options and returns static
// FormattingOptionsProvider based on them.
func NewFormattingOptionsProvider(f *FormattingOptions, formatBytes []byte) (FormattingOptionsProvider, error) {
	formatVersion := f.Version

	if formatVersion < minSupportedReadVersion || formatVersion > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, minSupportedReadVersion, maxSupportedReadVersion)
	}

	if formatVersion < minSupportedWriteVersion || formatVersion > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, minSupportedWriteVersion, maxSupportedWriteVersion)
	}

	actualIndexVersion := f.IndexVersion
	if actualIndexVersion == 0 {
		actualIndexVersion = legacyIndexVersion
	}

	if actualIndexVersion < index.Version1 || actualIndexVersion > index.Version2 {
		return nil, errors.Errorf("index version %v is not supported", actualIndexVersion)
	}

	h, err := hashing.CreateHashFunc(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := encryption.CreateEncryptor(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create encryptor")
	}

	contentID := h(nil, gather.FromSlice(nil))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = e.Encrypt(gather.FromSlice(nil), contentID, &tmp)
	if err != nil {
		return nil, errors.Wrap(err, "invalid encryptor")
	}

	return &formattingOptionsProvider{
		FormattingOptions: f,

		h:                   h,
		e:                   e,
		actualIndexVersion:  actualIndexVersion,
		actualFormatVersion: f.Version,
		formatBytes:         formatBytes,
	}, nil
}

func (f *formattingOptionsProvider) Encryptor() encryption.Encryptor {
	return f.e
}

func (f *formattingOptionsProvider) HashFunc() hashing.HashFunc {
	return f.h
}

func (f *formattingOptionsProvider) WriteIndexVersion() int {
	return f.actualIndexVersion
}

func (f *formattingOptionsProvider) MaxIndexBlobSize() int64 {
	return int64(f.MaxPackSize)
}

func (f *formattingOptionsProvider) MaxPackBlobSize() int {
	return f.MaxPackSize
}

func (f *formattingOptionsProvider) GetEpochManagerParameters() epoch.Parameters {
	return f.EpochParameters
}

func (f *formattingOptionsProvider) IndexShardSize() int {
	return defaultIndexShardSize
}

func (f *formattingOptionsProvider) RepositoryFormatBytes() []byte {
	return f.formatBytes
}

var _ FormattingOptionsProvider = (*formattingOptionsProvider)(nil)
