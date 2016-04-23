package config

import (
	"encoding/json"
	"io"

	"github.com/kopia/kopia/blob"
)

type Config struct {
	Storage blob.StorageConfiguration `json:"storage"`
}

func (cfg *Config) SaveTo(w io.Writer) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (cfg *Config) Load(r io.Reader) error {
	return json.NewDecoder(r).Decode(cfg)
}
