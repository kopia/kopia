package repo

import (
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
)

// ClientOptions contains client-specific options that are persisted in local configuration file.
type ClientOptions struct {
	Hostname string `json:"hostname"`
	Username string `json:"username"`

	ReadOnly bool `json:"readonly,omitempty"`

	// Description is human-readable description of the repository to use in the UI.
	Description string `json:"description,omitempty"`

	EnableActions bool `json:"enableActions"`
}

// ApplyDefaults returns a copy of ClientOptions with defaults filled out.
func (o ClientOptions) ApplyDefaults(ctx context.Context, defaultDesc string) ClientOptions {
	if o.Hostname == "" {
		o.Hostname = GetDefaultHostName(ctx)
	}

	if o.Username == "" {
		o.Username = GetDefaultUserName(ctx)
	}

	if o.Description == "" {
		o.Description = defaultDesc
	}

	return o
}

// Override returns ClientOptions that overrides fields present in the provided ClientOptions.
func (o ClientOptions) Override(other ClientOptions) ClientOptions {
	if other.Description != "" {
		o.Description = other.Description
	}

	if other.Hostname != "" {
		o.Hostname = other.Hostname
	}

	if other.Username != "" {
		o.Username = other.Username
	}

	if other.ReadOnly {
		o.ReadOnly = other.ReadOnly
	}

	return o
}

// LocalConfig is a configuration of Kopia stored in a configuration file.
type LocalConfig struct {
	// APIServer is only provided for remote repository.
	APIServer *APIServerInfo `json:"apiServer,omitempty"`

	// Storage is only provided for direct repository access.
	Storage *blob.ConnectionInfo `json:"storage,omitempty"`

	Caching *content.CachingOptions `json:"caching,omitempty"`

	ClientOptions
}

// repositoryObjectFormat describes the format of objects in a repository.
type repositoryObjectFormat struct {
	content.FormattingOptions
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

	return errors.Wrap(err, "error saving local config")
}

// loadConfigFromFile reads the local configuration from the specified file.
func loadConfigFromFile(fileName string) (*LocalConfig, error) {
	f, err := os.Open(fileName) //nolint:gosec
	if err != nil {
		return nil, errors.Wrap(err, "error loading config file")
	}
	defer f.Close() //nolint:errcheck,gosec

	var lc LocalConfig

	if err := lc.Load(f); err != nil {
		return nil, err
	}

	return &lc, nil
}
