package repo

import (
	"crypto/rand"
	"encoding/json"
	"io"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	UniqueID                    []byte // force the use of particular unique ID for metadata manager
	MetadataEncryptionAlgorithm string // identifier of encryption algorithm
	KeyDerivationAlgorithm      string // identifier of key derivation algorithm

	BlockFormat         string // identifier of object format
	ObjectHMACSecret    []byte // force the use of particular object HMAC secret
	ObjectEncryptionKey []byte // force the use of particular object encryption key

	Splitter               string // splitter used to break objects into storage blocks
	MinBlockSize           int    // minimum block size used with dynamic splitter
	AvgBlockSize           int    // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize           int    // maximum size of storage block
	MaxPackedContentLength int    // maximum size of object to be considered for storage in a pack

	// test-only
	noHMAC bool // disable HMAC
}

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(st storage.Storage, opt *NewRepositoryOptions, creds auth.Credentials) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	format := metadataFormatFromOptions(opt)

	km, err := auth.NewKeyManager(creds, format.SecurityOptions)
	if err != nil {
		return err
	}

	formatBytes, err := json.Marshal(&format)
	if err != nil {
		return err
	}

	if err := st.PutBlock(MetadataBlockPrefix+formatBlockID, formatBytes); err != nil {
		return err
	}

	mm, err := newMetadataManager(st, &format, km)
	if err != nil {
		return err
	}

	// Write encrypted repository configuration block.
	rc := config.EncryptedRepositoryConfig{
		Format: repositoryObjectFormatFromOptions(opt),
	}

	if err := mm.putJSON(repositoryConfigBlockID, &rc); err != nil {
		return err
	}

	return nil
}

func metadataFormatFromOptions(opt *NewRepositoryOptions) config.MetadataFormat {
	return config.MetadataFormat{
		SecurityOptions: auth.SecurityOptions{
			KeyDerivationAlgorithm: applyDefaultString(opt.KeyDerivationAlgorithm, auth.DefaultKeyDerivationAlgorithm),
			UniqueID:               applyDefaultRandomBytes(opt.UniqueID, 32),
		},
		Version:             "1",
		EncryptionAlgorithm: applyDefaultString(opt.MetadataEncryptionAlgorithm, DefaultMetadataEncryptionAlgorithm),
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) config.RepositoryObjectFormat {
	f := config.RepositoryObjectFormat{
		Version:                1,
		Splitter:               applyDefaultString(opt.Splitter, object.DefaultSplitter),
		BlockFormat:            applyDefaultString(opt.BlockFormat, block.DefaultFormat),
		HMACSecret:             applyDefaultRandomBytes(opt.ObjectHMACSecret, 32),
		MasterKey:              applyDefaultRandomBytes(opt.ObjectEncryptionKey, 32),
		MaxBlockSize:           applyDefaultInt(opt.MaxBlockSize, 20<<20),          // 20MiB
		MinBlockSize:           applyDefaultInt(opt.MinBlockSize, 10<<20),          // 10MiB
		AvgBlockSize:           applyDefaultInt(opt.AvgBlockSize, 16<<20),          // 16MiB
		MaxPackedContentLength: applyDefaultInt(opt.MaxPackedContentLength, 4<<20), // 3 MB
	}

	if opt.noHMAC {
		f.HMACSecret = nil
	}

	return f
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b)
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
