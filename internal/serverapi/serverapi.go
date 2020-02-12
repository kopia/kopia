// Package serverapi contains GO types corresponding to Kopia server API.
package serverapi

import (
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// StatusResponse is the response of 'status' HTTP API command.
type StatusResponse struct {
	Connected   bool   `json:"connected"`
	ConfigFile  string `json:"configFile,omitempty"`
	CacheDir    string `json:"cacheDir,omitempty"`
	Hash        string `json:"hash,omitempty"`
	Encryption  string `json:"encryption,omitempty"`
	Splitter    string `json:"splitter,omitempty"`
	MaxPackSize int    `json:"maxPackSize,omitempty"`
	Storage     string `json:"storage,omitempty"`
}

// SourcesResponse is the response of 'sources' HTTP API command.
type SourcesResponse struct {
	Sources []*SourceStatus `json:"sources"`
}

// SourceStatus describes the status of a single source.
type SourceStatus struct {
	Source           snapshot.SourceInfo        `json:"source"`
	Status           string                     `json:"status"`
	SchedulingPolicy policy.SchedulingPolicy    `json:"schedule"`
	LastSnapshotTime time.Time                  `json:"lastSnapshotTime,omitempty"`
	NextSnapshotTime *time.Time                 `json:"nextSnapshotTime,omitempty"`
	UploadCounters   *snapshotfs.UploadCounters `json:"upload,omitempty"`
}

// PolicyListEntry describes single policy.
type PolicyListEntry struct {
	ID     string              `json:"id"`
	Target snapshot.SourceInfo `json:"target"`
	Policy *policy.Policy      `json:"policy"`
}

// PoliciesResponse is the response of 'policies' HTTP API command.
type PoliciesResponse struct {
	Policies []*PolicyListEntry `json:"policies"`
}

// Empty represents empty request/response.
type Empty struct {
}

// ErrorResponse represents error response.
type ErrorResponse struct {
	Error string `json:"error"`
}

// SourceActionResponse is a per-source response.
type SourceActionResponse struct {
	Success bool `json:"success"`
}

// MultipleSourceActionResponse contains per-source responses for all sources targeted by API command.
type MultipleSourceActionResponse struct {
	Sources map[string]SourceActionResponse `json:"sources"`
}

// CreateRequest contains request to create a repository in a given storage
type CreateRequest struct {
	Storage              blob.ConnectionInfo       `json:"storage"`
	Password             string                    `json:"password"`
	CacheOptions         content.CachingOptions    `json:"cacheOptions"`
	NewRepositoryOptions repo.NewRepositoryOptions `json:"options"`
}

// ConnectRequest contains request to connect to a repository.
type ConnectRequest struct {
	Storage      blob.ConnectionInfo    `json:"storage"`
	Password     string                 `json:"password"`
	CacheOptions content.CachingOptions `json:"cacheOptions"`
}
