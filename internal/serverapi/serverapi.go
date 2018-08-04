package serverapi

import (
	"time"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/snapshot"
)

// StatusResponse is the response of 'status' HTTP API command.
type StatusResponse struct {
	ConfigFile      string                  `json:"configFile"`
	CacheDir        string                  `json:"cacheDir"`
	BlockFormatting block.FormattingOptions `json:"blockFormatting"`
	Storage         string                  `json:"storage"`
}

// SourcesResponse is the response of 'sources' HTTP API command.
type SourcesResponse struct {
	Sources []SourceStatus `json:"sources"`
}

// SourceStatus describes the status of a single source.
type SourceStatus struct {
	Source           snapshot.SourceInfo `json:"source"`
	Status           string              `json:"status"`
	Policy           *snapshot.Policy    `json:"policy"`
	LastSnapshotSize int64               `json:"lastSnapshotSize,omitempty"`
	LastSnapshotTime time.Time           `json:"lastSnapshotTime,omitempty"`
	NextSnapshotTime time.Time           `json:"nextSnapshotTime,omitempty"`

	UploadStatus struct {
		UploadingPath          string `json:"path,omitempty"`
		UploadingPathCompleted int64  `json:"pathCompleted,omitempty"`
		UploadingPathTotal     int64  `json:"pathTotal,omitempty"`
	} `json:"upload"`
}

// PolicyListEntry describes single policy.
type PolicyListEntry struct {
	ID     string              `json:"id"`
	Target snapshot.SourceInfo `json:"target"`
	Policy *snapshot.Policy    `json:"policy"`
}

// PoliciesResponse is the response of 'policies' HTTP API command.
type PoliciesResponse struct {
	Policies []*PolicyListEntry `json:"policies"`
}

// Empty represents empty request/response.
type Empty struct {
}

// SourceActionResponse is a per-source response.
type SourceActionResponse struct {
	Success bool `json:"success"`
}

// MultipleSourceActionResponse contains per-source responses for all sources targeted by API command.
type MultipleSourceActionResponse struct {
	Sources map[string]SourceActionResponse `json:"sources"`
}
