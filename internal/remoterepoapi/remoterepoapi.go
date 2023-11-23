// Package remoterepoapi contains requests and responses for remote repository API.
package remoterepoapi

import (
	"encoding/json"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/manifest"
)

// Parameters encapsulates all parameters for repository.
// returned by /api/v1/repo/parameters.
type Parameters struct {
	HashFunction               string `json:"hash"`
	HMACSecret                 []byte `json:"hmacSecret"`
	SupportsContentCompression bool   `json:"supportsContentCompression"`

	format.ObjectFormat
}

// GetHashFunction returns the name of the hash function for remote repository.
func (p *Parameters) GetHashFunction() string { return p.HashFunction }

// GetHmacSecret returns the HMAC secret for the remote repository.
func (p *Parameters) GetHmacSecret() []byte { return p.HMACSecret }

// ManifestWithMetadata represents manifest payload and metadata.
type ManifestWithMetadata struct {
	Payload  json.RawMessage         `json:"payload"`
	Metadata *manifest.EntryMetadata `json:"metadata"`
}

// PrefetchContentsRequest represents a request to prefetch contents.
type PrefetchContentsRequest struct {
	ContentIDs []content.ID `json:"contents"`
	Hint       string       `json:"hint"`
}

// PrefetchContentsResponse represents a response from request to prefetch contents.
type PrefetchContentsResponse struct {
	ContentIDs []content.ID `json:"contents"`
}

// ApplyRetentionPolicyRequest represents a request to apply retention policy to a given source path.
type ApplyRetentionPolicyRequest struct {
	SourcePath   string `json:"sourcePath"`
	ReallyDelete bool   `json:"reallyDelete"`
}

// ApplyRetentionPolicyResponse represents a response to a request to apply retention policy.
type ApplyRetentionPolicyResponse struct {
	ManifestIDs []manifest.ID `json:"manifests"`
}
