package repo

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/internal/config"
)

// NewRepositoryOptions specifies options that apply to newly created repositories.
// All fields are optional, when not provided, reasonable defaults will be used.
type NewRepositoryOptions struct {
	MetadataEncryptionAlgorithm string // identifier of encryption algorithm
	ObjectFormat                string // identifier of object format
	KeyDerivationAlgorithm      string // identifier of key derivation algorithm

	MaxInlineContentLength int    // maximum size of object to be considered for inline storage within ObjectID
	Splitter               string // splitter used to break objects into storage blocks
	MinBlockSize           int    // minimum block size used with dynamic splitter
	AvgBlockSize           int    // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize           int    // maximum size of storage block
}

// Initialize creates initial repository data structures in the specified storage with given credentials.
func Initialize(st blob.Storage, opt *NewRepositoryOptions, creds auth.Credentials) error {
	if opt == nil {
		opt = &NewRepositoryOptions{}
	}

	mm := MetadataManager{
		storage: st,
		format:  metadataFormatFromOptions(opt),
	}

	var err error
	mm.masterKey, err = creds.GetMasterKey(mm.format.Options)
	if err != nil {
		return err
	}

	formatBytes, err := json.Marshal(&mm.format)
	if err != nil {
		return err
	}

	if err := st.PutBlock(MetadataBlockPrefix+formatBlockID, formatBytes, blob.PutOptionsOverwrite); err != nil {
		return err
	}

	if err := mm.initCrypto(); err != nil {
		return fmt.Errorf("unable to initialize crypto: %v", err)
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
		Options: auth.Options{
			KeyDerivationAlgorithm: applyDefaultString(opt.KeyDerivationAlgorithm, auth.DefaultKeyDerivationAlgorithm),
			UniqueID:               randomBytes(32),
		},
		Version:             "1",
		EncryptionAlgorithm: applyDefaultString(opt.MetadataEncryptionAlgorithm, DefaultMetadataEncryptionAlgorithm),
	}
}

func repositoryObjectFormatFromOptions(opt *NewRepositoryOptions) config.RepositoryObjectFormat {
	return config.RepositoryObjectFormat{
		Version:                1,
		Splitter:               applyDefaultString(opt.Splitter, DefaultObjectSplitter),
		ObjectFormat:           applyDefaultString(opt.ObjectFormat, DefaultObjectFormat),
		Secret:                 randomBytes(32),
		MasterKey:              randomBytes(32),
		MaxInlineContentLength: applyDefaultInt(opt.MaxInlineContentLength, 32<<10), // 32KiB
		MaxBlockSize:           applyDefaultInt(opt.MaxBlockSize, 20<<20),           // 20MiB
		MinBlockSize:           applyDefaultInt(opt.MinBlockSize, 10<<20),           // 10MiB
		AvgBlockSize:           applyDefaultInt(opt.AvgBlockSize, 16<<20),           // 16MiB
	}
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
