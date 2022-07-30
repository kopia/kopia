package format

import (
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

const (
	minValidPackSize = 10 << 20
	maxValidPackSize = 120 << 20

	defaultIndexShardSize = 16e6 // slightly less than 2^24, which lets index use 24-bit/3-byte indexes

	// CurrentWriteVersion is the version of the repository applied to new repositories.
	CurrentWriteVersion = FormatVersion3

	// MinSupportedWriteVersion is the minimum version that this kopia client can write.
	MinSupportedWriteVersion = FormatVersion1

	// MaxSupportedWriteVersion is the maximum version that this kopia client can write.
	MaxSupportedWriteVersion = FormatVersion3

	// MinSupportedReadVersion is the minimum version that this kopia client can read.
	MinSupportedReadVersion = FormatVersion1

	// MaxSupportedReadVersion is the maximum version that this kopia client can read.
	MaxSupportedReadVersion = FormatVersion3

	legacyIndexVersion = index.Version1
)

// Version denotes content format version.
type Version int

// Supported format versions.
const (
	FormatVersion1 Version = 1
	FormatVersion2 Version = 2 // new in v0.9
	FormatVersion3 Version = 3 // new in v0.11

	MaxFormatVersion = FormatVersion3
)

// Provider provides current formatting options. The options returned
// should not be cached for more than a few seconds as they are subject to change.
type Provider interface {
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
	FormatVersion() Version
	MaxPackBlobSize() int
	RepositoryFormatBytes() []byte
	Struct() ContentFormat
}

type formattingOptionsProvider struct {
	*ContentFormat

	h                   hashing.HashFunc
	e                   encryption.Encryptor
	actualFormatVersion Version
	actualIndexVersion  int
	formatBytes         []byte
}

func (f *formattingOptionsProvider) FormatVersion() Version {
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

func (f *formattingOptionsProvider) Struct() ContentFormat {
	return *f.ContentFormat
}

// NewFormattingOptionsProvider validates the provided formatting options and returns static
// FormattingOptionsProvider based on them.
func NewFormattingOptionsProvider(f *ContentFormat, formatBytes []byte) (Provider, error) {
	formatVersion := f.Version

	if formatVersion < MinSupportedReadVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedReadVersion, MaxSupportedReadVersion)
	}

	if formatVersion < MinSupportedWriteVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedWriteVersion, MaxSupportedWriteVersion)
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
		ContentFormat: f,

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

var _ Provider = (*formattingOptionsProvider)(nil)
