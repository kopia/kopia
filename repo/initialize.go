package repo

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
)

// BuildInfo is the build information of Kopia.
var (
	BuildInfo    = "unknown"
	BuildVersion = "unknown"
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

	BlockFormat         string // identifier of object format
	ObjectHMACSecret    []byte // force the use of particular object HMAC secret
	ObjectEncryptionKey []byte // force the use of particular object encryption key

	Splitter     string // splitter used to break objects into storage blocks
	MinBlockSize int    // minimum block size used with dynamic splitter
	AvgBlockSize int    // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize int    // maximum size of storage block

	// test-only
	noHMAC bool // disable HMAC
}

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(ctx context.Context, st storage.Storage, opt *NewRepositoryOptions, creds auth.Credentials) error {
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
	km, err := auth.NewKeyManager(creds, format.SecurityOptions)
	if err != nil {
		return err
	}

	if err := encryptFormatBytes(format, repositoryObjectFormatFromOptions(opt), km); err != nil {
		return err
	}

	if err := writeFormatBlock(ctx, st, format); err != nil {
		return err
	}

	return nil
}

func formatBlockFromOptions(opt *NewRepositoryOptions) *formatBlock {
	return &formatBlock{
		Tool:      "https://github.com/kopia/kopia",
		BuildInfo: BuildInfo,
		SecurityOptions: auth.SecurityOptions{
			KeyDerivationAlgorithm: applyDefaultString(opt.KeyDerivationAlgorithm, auth.DefaultKeyDerivationAlgorithm),
			UniqueID:               applyDefaultRandomBytes(opt.UniqueID, 32),
		},
		Version:             "1",
		EncryptionAlgorithm: applyDefaultString(opt.MetadataEncryptionAlgorithm, DefaultEncryptionAlgorithm),
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) *config.RepositoryObjectFormat {
	f := &config.RepositoryObjectFormat{
		FormattingOptions: block.FormattingOptions{
			Version:     1,
			BlockFormat: applyDefaultString(opt.BlockFormat, block.DefaultFormat),
			HMACSecret:  applyDefaultRandomBytes(opt.ObjectHMACSecret, 32),
			MasterKey:   applyDefaultRandomBytes(opt.ObjectEncryptionKey, 32),
			MaxPackSize: applyDefaultInt(opt.MaxBlockSize, 20<<20), // 20 MB
		},
		Splitter:     applyDefaultString(opt.Splitter, object.DefaultSplitter),
		MaxBlockSize: applyDefaultInt(opt.MaxBlockSize, 20<<20), // 20MiB
		MinBlockSize: applyDefaultInt(opt.MinBlockSize, 10<<20), // 10MiB
		AvgBlockSize: applyDefaultInt(opt.AvgBlockSize, 16<<20), // 16MiB
	}

	if opt.noHMAC {
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
