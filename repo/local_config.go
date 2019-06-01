package repo

import (
	"encoding/json"
	"io"
	"os"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/block"
	"github.com/kopia/kopia/repo/object"
)

// LocalConfig is a configuration of Kopia stored in a configuration file.
type LocalConfig struct {
	Storage blob.ConnectionInfo  `json:"storage"`
	Caching block.CachingOptions `json:"caching"`
}

// repositoryObjectFormat describes the format of objects in a repository.
type repositoryObjectFormat struct {
	block.FormattingOptions
	object.Format
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

// loadConfigFromFile reads the local configuration from the specified file.
func loadConfigFromFile(fileName string) (*LocalConfig, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	var lc LocalConfig

	if err := lc.Load(f); err != nil {
		return nil, err
	}

	return &lc, nil
}
