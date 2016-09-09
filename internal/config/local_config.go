// Package config stores persistent configuration of Kopia.
package config

import (
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/blob/caching"
	"github.com/kopia/kopia/vault"

	"github.com/kopia/kopia/blob"
)

// LocalConfig is a configuration of Kopia.
type LocalConfig struct {
	VaultConnection *vault.Config        `json:"vault,omitempty"`
	RepoConnection  *blob.ConnectionInfo `json:"repository,omitempty"` // can be nil indicating the same connection as for the vault
	Caching         *caching.Options     `json:"caching,omitempty"`
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
