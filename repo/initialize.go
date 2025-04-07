package repo

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/splitter"
)

// BuildInfo is the build information of Kopia.
//
//nolint:gochecknoglobals
var (
	BuildInfo       = "unknown"
	BuildVersion    = "v0-unofficial"
	BuildGitHubRepo = ""
)

const (
	hmacSecretLength = 32
	masterKeyLength  = 32
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID                          []byte               `json:"uniqueID"` // force the use of particular unique ID
	BlockFormat                       format.ContentFormat `json:"blockFormat"`
	DisableHMAC                       bool                 `json:"disableHMAC"`
	ObjectFormat                      format.ObjectFormat  `json:"objectFormat"` // object format
	RetentionMode                     blob.RetentionMode   `json:"retentionMode,omitempty"`
	RetentionPeriod                   time.Duration        `json:"retentionPeriod,omitempty"`
	FormatBlockKeyDerivationAlgorithm string               `json:"formatBlockKeyDerivationAlgorithm,omitempty"`
}

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(ctx context.Context, st blob.Storage, opt *NewRepositoryOptions, password string) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	formatBlob := formatBlobFromOptions(opt)
	blobcfg := blobCfgBlobFromOptions(opt)

	repoConfig, err := repositoryObjectFormatFromOptions(opt)
	if err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	//nolint:wrapcheck
	return format.Initialize(ctx, st, formatBlob, repoConfig, blobcfg, password)
}

func formatBlobFromOptions(opt *NewRepositoryOptions) *format.KopiaRepositoryJSON {
	return &format.KopiaRepositoryJSON{
		Tool:                   "https://github.com/kopia/kopia",
		BuildInfo:              BuildInfo,
		BuildVersion:           BuildVersion,
		KeyDerivationAlgorithm: opt.FormatBlockKeyDerivationAlgorithm,
		UniqueID:               applyDefaultRandomBytes(opt.UniqueID, format.UniqueIDLengthBytes),
		EncryptionAlgorithm:    format.DefaultFormatEncryption,
	}
}

func blobCfgBlobFromOptions(opt *NewRepositoryOptions) format.BlobStorageConfiguration {
	return format.BlobStorageConfiguration{
		RetentionMode:   opt.RetentionMode,
		RetentionPeriod: opt.RetentionPeriod,
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) (*format.RepositoryConfig, error) {
	fv := opt.BlockFormat.Version
	if fv == 0 {
		switch os.Getenv("KOPIA_REPOSITORY_FORMAT_VERSION") {
		case "1":
			fv = format.FormatVersion1
		case "2":
			fv = format.FormatVersion2
		case "3":
			fv = format.FormatVersion3
		default:
			fv = format.FormatVersion3
		}
	}

	f := &format.RepositoryConfig{
		ContentFormat: format.ContentFormat{
			Hash:               applyDefaultString(opt.BlockFormat.Hash, hashing.DefaultAlgorithm),
			Encryption:         applyDefaultString(opt.BlockFormat.Encryption, encryption.DefaultAlgorithm),
			ECC:                applyDefaultString(opt.BlockFormat.ECC, ecc.DefaultAlgorithm),
			ECCOverheadPercent: applyDefaultIntRange(opt.BlockFormat.ECCOverheadPercent, 0, 100), //nolint:mnd
			HMACSecret:         applyDefaultRandomBytes(opt.BlockFormat.HMACSecret, hmacSecretLength),
			MasterKey:          applyDefaultRandomBytes(opt.BlockFormat.MasterKey, masterKeyLength),
			MutableParameters: format.MutableParameters{
				Version:         fv,
				MaxPackSize:     applyDefaultInt(opt.BlockFormat.MaxPackSize, 20<<20), //nolint:mnd
				IndexVersion:    applyDefaultInt(opt.BlockFormat.IndexVersion, content.DefaultIndexVersion),
				EpochParameters: opt.BlockFormat.EpochParameters,
			},
			EnablePasswordChange: opt.BlockFormat.EnablePasswordChange,
		},
		ObjectFormat: format.ObjectFormat{
			Splitter: applyDefaultString(opt.ObjectFormat.Splitter, splitter.DefaultAlgorithm),
		},
	}

	if opt.DisableHMAC {
		f.HMACSecret = nil
	}

	if fv == format.FormatVersion1 || f.ContentFormat.ECCOverheadPercent == 0 {
		f.ContentFormat.ECC = ""
		f.ContentFormat.ECCOverheadPercent = 0
	}

	if err := f.ContentFormat.ResolveFormatVersion(); err != nil {
		return nil, errors.Wrap(err, "error resolving format version")
	}

	return f, nil
}

func applyDefaultInt(v, def int) int {
	if v == 0 {
		return def
	}

	return v
}

func applyDefaultIntRange(v, minValue, maxValue int) int {
	if v < minValue {
		return minValue
	} else if v > maxValue {
		return maxValue
	}

	return v
}

func applyDefaultString(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b) //nolint:errcheck

	return b
}

func applyDefaultRandomBytes(b []byte, n int) []byte {
	if b == nil {
		return randomBytes(n)
	}

	return b
}
