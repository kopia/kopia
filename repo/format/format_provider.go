package format

import (
	"context"

	"github.com/pkg/errors"

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
	GetMutableParameters(ctx context.Context) (MutableParameters, error)
	GetCachedMutableParameters() MutableParameters
	SupportsPasswordChange() bool
	GetMasterKey() []byte

	RepositoryFormatBytes(ctx context.Context) ([]byte, error)
}

type formattingOptionsProvider struct {
	*ContentFormat

	h           hashing.HashFunc
	e           encryption.Encryptor
	formatBytes []byte
}

// NewFormattingOptionsProvider validates the provided formatting options and returns static
// FormattingOptionsProvider based on them.
func NewFormattingOptionsProvider(f0 *ContentFormat, formatBytes []byte) (Provider, error) {
	clone := *f0
	f := &clone
	formatVersion := f.Version

	if formatVersion < MinSupportedReadVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedReadVersion, MaxSupportedReadVersion)
	}

	if formatVersion < MinSupportedWriteVersion || formatVersion > CurrentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", formatVersion, MinSupportedWriteVersion, MaxSupportedWriteVersion)
	}

	if f.IndexVersion == 0 {
		f.IndexVersion = legacyIndexVersion
	}

	if f.IndexVersion < index.Version1 || f.IndexVersion > index.Version2 {
		return nil, errors.Errorf("index version %v is not supported", f.IndexVersion)
	}

	// apply default
	if f.MaxPackSize == 0 {
		// legacy only, apply default
		f.MaxPackSize = 20 << 20 //nolint:mnd
	}

	h, err := hashing.CreateHashFunc(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := encryption.CreateEncryptor(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create encryptor")
	}

	if f.GetECCAlgorithm() != "" && f.GetECCOverheadPercent() > 0 {
		eccEncryptor, err := ecc.CreateEncryptor(f)
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

	return &formattingOptionsProvider{
		ContentFormat: f,

		h:           h,
		e:           e,
		formatBytes: formatBytes,
	}, nil
}

func (f *formattingOptionsProvider) Encryptor() encryption.Encryptor {
	return f.e
}

func (f *formattingOptionsProvider) HashFunc() hashing.HashFunc {
	return f.h
}

func (f *formattingOptionsProvider) RepositoryFormatBytes(ctx context.Context) ([]byte, error) {
	if f.SupportsPasswordChange() {
		return nil, nil
	}

	return f.formatBytes, nil
}

var _ Provider = (*formattingOptionsProvider)(nil)
