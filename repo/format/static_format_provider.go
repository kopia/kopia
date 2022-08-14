package format

import (
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

const (
	minValidPackSize = 10 << 20
	maxValidPackSize = 120 << 20

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
	encryption.Parameters
	hashing.Parameters
	ecc.Parameters

	HashFunc() hashing.HashFunc
	Encryptor() encryption.Encryptor

	// this is typically cached, but sometimes refreshes MutableParameters from
	// the repository so the results should not be cached.
	GetMutableParameters() (MutableParameters, error)
	SupportsPasswordChange() bool
	GetMasterKey() []byte

	RepositoryFormatBytes() ([]byte, error)

	LoadedTime() time.Time // time when the format was loaded
}

type staticProvider struct {
	rc          *RepositoryConfig
	h           hashing.HashFunc
	e           encryption.Encryptor
	formatBytes []byte
	loadedTime  time.Time
}

// GetEncryptionAlgorithm implements encryption.Parameters.
func (f *staticProvider) GetEncryptionAlgorithm() string {
	return f.rc.Encryption
}

// GetMasterKey implements encryption.Parameters.
func (f *staticProvider) GetMasterKey() []byte {
	return f.rc.MasterKey
}

// GetHashFunction implements hashing.Parameters.
func (f *staticProvider) GetHashFunction() string {
	return f.rc.Hash
}

// GetHmacSecret implements hashing.Parameters.
func (f *staticProvider) GetHmacSecret() []byte {
	return f.rc.HMACSecret
}

// GetMutableParameters implements FormattingOptionsProvider.
func (f *staticProvider) GetMutableParameters() (MutableParameters, error) {
	return f.rc.MutableParameters, nil
}

// SupportsPasswordChange implements FormattingOptionsProvider.
func (f *staticProvider) SupportsPasswordChange() bool {
	return f.rc.EnablePasswordChange
}

func (f *staticProvider) FormatVersion() Version {
	return f.rc.Version
}

func (f *staticProvider) LoadedTime() time.Time {
	return f.loadedTime
}

// NewStaticProvider validates the provided formatting options and returns static
// FormattingOptionsProvider based on them.
func NewStaticProvider(rc *RepositoryConfig, formatBytes []byte) (Provider, error) {
	formatVersion := rc.Version

	if formatVersion < MinSupportedReadVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedReadVersion, MaxSupportedReadVersion)
	}

	if formatVersion < MinSupportedWriteVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedWriteVersion, MaxSupportedWriteVersion)
	}

	if rc.ContentFormat.IndexVersion == 0 {
		rc.ContentFormat.IndexVersion = legacyIndexVersion
	}

	if rc.ContentFormat.IndexVersion < index.Version1 || rc.ContentFormat.IndexVersion > index.Version2 {
		return nil, errors.Errorf("index version %v is not supported", rc.ContentFormat.IndexVersion)
	}

	// apply default
	if rc.ContentFormat.MaxPackSize == 0 {
		// legacy only, apply default
		rc.ContentFormat.MaxPackSize = 20 << 20 //nolint:gomnd
	}

	h, err := hashing.CreateHashFunc(rc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := encryption.CreateEncryptor(rc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create encryptor")
	}

	if rc.GetECCAlgorithm() != "" && rc.GetECCOverheadPercent() > 0 {
		eccEncryptor, err := ecc.CreateEncryptor(rc) //nolint:govet
		if err != nil {
			return nil, errors.Wrap(err, "unable to create ECC")
		}

		e = &encryptorWrapper{
			impl: e,
			next: eccEncryptor,
		}
	}

	contentID := h(nil, gather.FromSlice(nil))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = e.Encrypt(gather.FromSlice(nil), contentID, &tmp)
	if err != nil {
		return nil, errors.Wrap(err, "invalid encryptor")
	}

	return &staticProvider{
		rc: rc,

		h:           h,
		e:           e,
		formatBytes: formatBytes,

		loadedTime: clock.Now(),
	}, nil
}

func (f *staticProvider) Encryptor() encryption.Encryptor {
	return f.e
}

func (f *staticProvider) HashFunc() hashing.HashFunc {
	return f.h
}

func (f *staticProvider) RepositoryFormatBytes() ([]byte, error) {
	if f.rc.SupportsPasswordChange() {
		return nil, nil
	}

	return f.formatBytes, nil
}

var _ Provider = (*staticProvider)(nil)
