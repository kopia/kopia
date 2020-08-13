// Package remoterepoapi contains requests and responses for remote repository API.
package remoterepoapi

import (
	"encoding/json"

	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// Parameters encapsulates all parameters for repository.
// returned by /api/v1/repo/parameters.
type Parameters struct {
	HashFunction string `json:"hash"`
	HMACSecret   []byte `json:"hmacSecret"`

	object.Format
}

// GetHashFunction returns the name of the hash function for remote repository.
func (p *Parameters) GetHashFunction() string { return p.HashFunction }

// GetHMACSecret returns the HMAC secret for the remote repository.
func (p *Parameters) GetHMACSecret() []byte { return p.HMACSecret }

// ManifestWithMetadata represents manifest payload and metadata.
type ManifestWithMetadata struct {
	Payload  json.RawMessage         `json:"payload"`
	Metadata *manifest.EntryMetadata `json:"metadata"`
}
