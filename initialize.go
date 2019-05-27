package repo

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/kopia/repo/block"
	"github.com/kopia/repo/object"
	"github.com/kopia/repo/storage"
	"github.com/pkg/errors"
)

// BuildInfo is the build information of Kopia.
var (
	BuildInfo    = "unknown"
	BuildVersion = "v0-unofficial"
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID     []byte // force the use of particular unique ID
	BlockFormat  block.FormattingOptions
	DisableHMAC  bool
	ObjectFormat object.Format // object format
}

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(ctx context.Context, st storage.Storage, opt *NewRepositoryOptions, password string) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	// get the block - expect ErrBlockNotFound
	_, err := st.GetBlock(ctx, FormatBlockID, 0, -1)
	if err == nil {
		return fmt.Errorf("repository already initialized")
	}
	if err != storage.ErrBlockNotFound {
		return err
	}

	format := formatBlockFromOptions(opt)
	masterKey, err := format.deriveMasterKeyFromPassword(password)
	if err != nil {
		return errors.Wrap(err, "unable to derive master key")
	}

	if err := encryptFormatBytes(format, repositoryObjectFormatFromOptions(opt), masterKey, format.UniqueID); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := writeFormatBlock(ctx, st, format); err != nil {
		return errors.Wrap(err, "unable to write format block")
	}

	return nil
}

func formatBlockFromOptions(opt *NewRepositoryOptions) *formatBlock {
	f := &formatBlock{
		Tool:                   "https://github.com/kopia/kopia",
		BuildInfo:              BuildInfo,
		KeyDerivationAlgorithm: defaultKeyDerivationAlgorithm,
		UniqueID:               applyDefaultRandomBytes(opt.UniqueID, 32),
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
		FormattingOptions: block.FormattingOptions{
			Version:     1,
			Hash:        applyDefaultString(opt.BlockFormat.Hash, block.DefaultHash),
			Encryption:  applyDefaultString(opt.BlockFormat.Encryption, block.DefaultEncryption),
			HMACSecret:  applyDefaultRandomBytes(opt.BlockFormat.HMACSecret, 32),
			MasterKey:   applyDefaultRandomBytes(opt.BlockFormat.MasterKey, 32),
			MaxPackSize: applyDefaultInt(opt.BlockFormat.MaxPackSize, applyDefaultInt(opt.ObjectFormat.MaxBlockSize, 20<<20)), // 20 MB
		},
		Format: object.Format{
			Splitter:     applyDefaultString(opt.ObjectFormat.Splitter, object.DefaultSplitter),
			MaxBlockSize: applyDefaultInt(opt.ObjectFormat.MaxBlockSize, 20<<20), // 20MiB
			MinBlockSize: applyDefaultInt(opt.ObjectFormat.MinBlockSize, 10<<20), // 10MiB
			AvgBlockSize: applyDefaultInt(opt.ObjectFormat.AvgBlockSize, 16<<20), // 16MiB
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
