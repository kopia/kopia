package repo

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/kopia/repo/block"
	"github.com/kopia/repo/object"
	"github.com/kopia/repo/storage"
)

// BuildInfo is the build information of Kopia.
var (
	BuildInfo    = "unknown"
	BuildVersion = "v0-unofficial"
)

// DefaultEncryptionAlgorithm is the default algorithm for encrypting format block.
var DefaultEncryptionAlgorithm = "AES256_GCM"

// SupportedEncryptionAlgorithms lists all supported algorithms for encrypting format block.
var SupportedEncryptionAlgorithms = []string{DefaultEncryptionAlgorithm, "NONE"}

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID                    []byte // force the use of particular unique ID for metadata manager
	MetadataEncryptionAlgorithm string // identifier of encryption algorithm
	KeyDerivationAlgorithm      string // identifier of key derivation algorithm

	BlockFormat block.FormattingOptions
	DisableHMAC bool

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
		return fmt.Errorf("unable to derive master key: %v", err)
	}

	if err := encryptFormatBytes(format, repositoryObjectFormatFromOptions(opt), masterKey, format.UniqueID); err != nil {
		return fmt.Errorf("unable to encrypt format bytes: %v", err)
	}

	if err := writeFormatBlock(ctx, st, format); err != nil {
		return fmt.Errorf("unable to write format block: %v", err)
	}

	return nil
}

func formatBlockFromOptions(opt *NewRepositoryOptions) *formatBlock {
	return &formatBlock{
		Tool:                   "https://github.com/kopia/kopia",
		BuildInfo:              BuildInfo,
		KeyDerivationAlgorithm: applyDefaultString(opt.KeyDerivationAlgorithm, DefaultKeyDerivationAlgorithm),
		UniqueID:               applyDefaultRandomBytes(opt.UniqueID, 32),
		Version:                "1",
		EncryptionAlgorithm:    applyDefaultString(opt.MetadataEncryptionAlgorithm, DefaultEncryptionAlgorithm),
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) *repositoryObjectFormat {
	f := &repositoryObjectFormat{
		FormattingOptions: block.FormattingOptions{
			Version:     1,
			BlockFormat: applyDefaultString(opt.BlockFormat.BlockFormat, block.DefaultFormat),
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
