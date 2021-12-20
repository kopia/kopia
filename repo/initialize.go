package repo

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/splitter"
)

// BuildInfo is the build information of Kopia.
// nolint:gochecknoglobals
var (
	BuildInfo       = "unknown"
	BuildVersion    = "v0-unofficial"
	BuildGitHubRepo = ""
)

const (
	hmacSecretLength = 32
	masterKeyLength  = 32
	uniqueIDLength   = 32
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID        []byte                    `json:"uniqueID"` // force the use of particular unique ID
	BlockFormat     content.FormattingOptions `json:"blockFormat"`
	DisableHMAC     bool                      `json:"disableHMAC"`
	ObjectFormat    object.Format             `json:"objectFormat"` // object format
	RetentionMode   blob.RetentionMode        `json:"retentionMode,omitempty"`
	RetentionPeriod time.Duration             `json:"retentionPeriod,omitempty"`
}

// ErrAlreadyInitialized indicates that repository has already been initialized.
var ErrAlreadyInitialized = errors.Errorf("repository already initialized")

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(ctx context.Context, st blob.Storage, opt *NewRepositoryOptions, password string) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	// get the blob - expect ErrNotFound
	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := st.GetBlob(ctx, FormatBlobID, 0, -1, &tmp)
	if err == nil {
		return ErrAlreadyInitialized
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Wrap(err, "unexpected error when checking for format blob")
	}

	err = st.GetBlob(ctx, BlobCfgBlobID, 0, -1, &tmp)
	if err == nil {
		return errors.Errorf("possible corruption: blobcfg blob exists, but format blob is not found")
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		return errors.Wrap(err, "unexpected error when checking for blobcfg blob")
	}

	format := formatBlobFromOptions(opt)
	blobcfg := blobCfgBlobFromOptions(opt)

	formatEncryptionKey, err := format.deriveFormatEncryptionKeyFromPassword(password)
	if err != nil {
		return errors.Wrap(err, "unable to derive format encryption key")
	}

	f, err := repositoryObjectFormatFromOptions(opt)
	if err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err = f.MutableParameters.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err = encryptFormatBytes(format, f, formatEncryptionKey, format.UniqueID); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := writeBlobCfgBlob(ctx, st, format, blobcfg, formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := writeFormatBlob(ctx, st, format, blobcfg); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	return nil
}

func formatBlobFromOptions(opt *NewRepositoryOptions) *formatBlob {
	return &formatBlob{
		Tool:                   "https://github.com/kopia/kopia",
		BuildInfo:              BuildInfo,
		BuildVersion:           BuildVersion,
		KeyDerivationAlgorithm: defaultKeyDerivationAlgorithm,
		UniqueID:               applyDefaultRandomBytes(opt.UniqueID, uniqueIDLength),
		EncryptionAlgorithm:    defaultFormatEncryption,
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) (*repositoryObjectFormat, error) {
	fv := opt.BlockFormat.Version
	if fv == 0 {
		switch os.Getenv("KOPIA_REPOSITORY_FORMAT_VERSION") {
		case "1":
			fv = content.FormatVersion1
		case "2":
			fv = content.FormatVersion2
		case "3":
			fv = content.FormatVersion3
		default:
			fv = content.FormatVersion3
		}
	}

	f := &repositoryObjectFormat{
		FormattingOptions: content.FormattingOptions{
			Hash:       applyDefaultString(opt.BlockFormat.Hash, hashing.DefaultAlgorithm),
			Encryption: applyDefaultString(opt.BlockFormat.Encryption, encryption.DefaultAlgorithm),
			HMACSecret: applyDefaultRandomBytes(opt.BlockFormat.HMACSecret, hmacSecretLength),
			MasterKey:  applyDefaultRandomBytes(opt.BlockFormat.MasterKey, masterKeyLength),
			MutableParameters: content.MutableParameters{
				Version:         fv,
				MaxPackSize:     applyDefaultInt(opt.BlockFormat.MaxPackSize, 20<<20), //nolint:gomnd
				IndexVersion:    applyDefaultInt(opt.BlockFormat.IndexVersion, content.DefaultIndexVersion),
				EpochParameters: opt.BlockFormat.EpochParameters,
			},
			EnablePasswordChange: opt.BlockFormat.EnablePasswordChange,
		},
		Format: object.Format{
			Splitter: applyDefaultString(opt.ObjectFormat.Splitter, splitter.DefaultAlgorithm),
		},
	}

	if opt.DisableHMAC {
		f.HMACSecret = nil
	}

	if err := f.FormattingOptions.ResolveFormatVersion(); err != nil {
		return nil, errors.Wrap(err, "error resolving format version")
	}

	return f, nil
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b) //nolint:errcheck

	return b
}

func applyDefaultInt(v, def int) int {
	if v == 0 {
		return def
	}

	return v
}

func applyDefaultString(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

func applyDefaultRandomBytes(b []byte, n int) []byte {
	if b == nil {
		return randomBytes(n)
	}

	return b
}
