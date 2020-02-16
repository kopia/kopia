package repo

import (
	"context"
	"crypto/rand"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
)

// BuildInfo is the build information of Kopia.
var (
	BuildInfo    = "unknown"
	BuildVersion = "v0-unofficial"
)

const (
	hmacSecretLength = 32
	masterKeyLength  = 32
	uniqueIDLength   = 32
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID     []byte                    `json:"uniqueID"` // force the use of particular unique ID
	BlockFormat  content.FormattingOptions `json:"blockFormat"`
	DisableHMAC  bool                      `json:"disableHMAC"`
	ObjectFormat object.Format             `json:"objectFormat"` // object format
}

// ErrAlreadyInitialized indicates that repository has already been initialized.
var ErrAlreadyInitialized = errors.Errorf("repository already initialized")

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(ctx context.Context, st blob.Storage, opt *NewRepositoryOptions, password string) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	// get the blob - expect ErrNotFound
	_, err := st.GetBlob(ctx, FormatBlobID, 0, -1)
	if err == nil {
		return ErrAlreadyInitialized
	}

	if err != blob.ErrBlobNotFound {
		return err
	}

	format := formatBlobFromOptions(opt)

	masterKey, err := format.deriveMasterKeyFromPassword(password)
	if err != nil {
		return errors.Wrap(err, "unable to derive master key")
	}

	if err := encryptFormatBytes(format, repositoryObjectFormatFromOptions(opt), masterKey, format.UniqueID); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := writeFormatBlob(ctx, st, format); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	return nil
}

func formatBlobFromOptions(opt *NewRepositoryOptions) *formatBlob {
	f := &formatBlob{
		Tool:                   "https://github.com/kopia/kopia",
		BuildInfo:              BuildInfo,
		KeyDerivationAlgorithm: defaultKeyDerivationAlgorithm,
		UniqueID:               applyDefaultRandomBytes(opt.UniqueID, uniqueIDLength),
		Version:                "1",
		EncryptionAlgorithm:    defaultFormatEncryption,
	}

	if opt.BlockFormat.Encryption == "NONE" {
		f.EncryptionAlgorithm = "NONE"
	}

	return f
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) *repositoryObjectFormat {
	f := &repositoryObjectFormat{
		FormattingOptions: content.FormattingOptions{
			Version:     1,
			Hash:        applyDefaultString(opt.BlockFormat.Hash, content.DefaultHash),
			Encryption:  applyDefaultString(opt.BlockFormat.Encryption, content.DefaultEncryption),
			HMACSecret:  applyDefaultRandomBytes(opt.BlockFormat.HMACSecret, hmacSecretLength), //nolint:gomnd
			MasterKey:   applyDefaultRandomBytes(opt.BlockFormat.MasterKey, masterKeyLength),   //nolint:gomnd
			MaxPackSize: applyDefaultInt(opt.BlockFormat.MaxPackSize, 20<<20),                  //nolint:gomnd
		},
		Format: object.Format{
			Splitter: applyDefaultString(opt.ObjectFormat.Splitter, object.DefaultSplitter),
		},
	}

	if opt.DisableHMAC {
		f.HMACSecret = nil
	}

	return f
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
