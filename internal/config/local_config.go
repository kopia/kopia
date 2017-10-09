package config

import (
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/storage"
)

// LocalConfig is a configuration of Kopia.
type LocalConfig struct {
	Connection     *RepositoryConnectionInfo `json:"connection,omitempty"`
	CacheDirectory string                    `json:"cacheDirectory,omitempty"`
}

// RepositoryObjectFormat describes the format of objects in a repository.
type RepositoryObjectFormat struct {
	Version                int    `json:"version,omitempty"`                // version number, must be "1"
	BlockFormat            string `json:"objectFormat,omitempty"`           // identifier of the block format
	HMACSecret             []byte `json:"secret,omitempty"`                 // HMAC secret used to generate encryption keys
	MasterKey              []byte `json:"masterKey,omitempty"`              // master encryption key (SIV-mode encryption only)
	Splitter               string `json:"splitter,omitempty"`               // splitter used to break objects into storage blocks
	MaxPackedContentLength int    `json:"maxPackedContentLength,omitempty"` // maximum size of object to be considered for storage in a pack

	MinBlockSize int `json:"minBlockSize,omitempty"` // minimum block size used with dynamic splitter
	AvgBlockSize int `json:"avgBlockSize,omitempty"` // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize int `json:"maxBlockSize,omitempty"` // maximum size of storage block
}

// RepositoryConnectionInfo represents JSON-serializable configuration of the repository connection, including master key.
type RepositoryConnectionInfo struct {
	ConnectionInfo storage.ConnectionInfo `json:"storage"`
	Key            []byte                 `json:"key,omitempty"`
}

// EncryptedRepositoryConfig contains the configuration of repository that's persisted in encrypted format.
type EncryptedRepositoryConfig struct {
	Format RepositoryObjectFormat `json:"format"`
}

// Load reads local configuration from the specified reader.
func (lc *LocalConfig) Load(r io.Reader) error {
	*lc = LocalConfig{}
	return json.NewDecoder(r).Decode(lc)
}

// Save writes the configuration to the specified writer.
func (lc *LocalConfig) Save(w io.Writer) error {
	b, err := json.MarshalIndent(lc, "", "  ")
	if err != nil {
		return nil
	}
	_, err = w.Write(b)
	return err
}

// LoadFromFile reads the local configuration from the specified file.
func LoadFromFile(fileName string) (*LocalConfig, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lc LocalConfig

	if err := lc.Load(f); err != nil {
		return nil, err
	}

	return &lc, nil
}
