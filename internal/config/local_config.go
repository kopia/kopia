package config

import (
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/storage"
)

// LocalConfig is a configuration of Kopia.
type LocalConfig struct {
	Connection *RepositoryConnectionInfo `json:"connection,omitempty"`
	Caching    block.CachingOptions      `json:"caching"`
}

// RepositoryObjectFormat describes the format of objects in a repository.
type RepositoryObjectFormat struct {
	block.FormattingOptions

	Splitter     string `json:"splitter,omitempty"`     // splitter used to break objects into storage blocks
	MinBlockSize int    `json:"minBlockSize,omitempty"` // minimum block size used with dynamic splitter
	AvgBlockSize int    `json:"avgBlockSize,omitempty"` // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize int    `json:"maxBlockSize,omitempty"` // maximum size of storage block
}

// RepositoryConnectionInfo represents JSON-serializable configuration of the repository connection, including master key.
type RepositoryConnectionInfo struct {
	ConnectionInfo storage.ConnectionInfo `json:"storage"`
	Key            []byte                 `json:"key,omitempty"`
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
