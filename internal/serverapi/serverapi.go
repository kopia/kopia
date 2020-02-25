// Package serverapi contains GO types corresponding to Kopia server API.
package serverapi

import (
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/manifest"
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
	LocalUsername string `json:"localUsername"`
	LocalHost     string `json:"localHost"`

	Sources []*SourceStatus `json:"sources"`
}

// SourceStatus describes the status of a single source.
type SourceStatus struct {
	Source           snapshot.SourceInfo        `json:"source"`
	Status           string                     `json:"status"`
	SchedulingPolicy policy.SchedulingPolicy    `json:"schedule"`
	LastSnapshotSize *int64                     `json:"lastSnapshotSize,omitempty"`
	LastSnapshotTime *time.Time                 `json:"lastSnapshotTime,omitempty"`
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

// APIErrorCode indicates machine-readable error code returned in API responses.
type APIErrorCode string

// Supported error codes.
const (
	ErrorInternal           APIErrorCode = "INTERNAL"
	ErrorAlreadyConnected   APIErrorCode = "ALREADY_CONNECTED"
	ErrorAlreadyInitialized APIErrorCode = "ALREADY_INITIALIZED"
	ErrorInvalidPassword    APIErrorCode = "INVALID_PASSWORD"
	ErrorInvalidToken       APIErrorCode = "INVALID_TOKEN"
	ErrorMalformedRequest   APIErrorCode = "MALFORMED_REQUEST"
	ErrorNotConnected       APIErrorCode = "NOT_CONNECTED"
	ErrorNotFound           APIErrorCode = "NOT_FOUND"
	ErrorNotInitialized     APIErrorCode = "NOT_INITIALIZED"
	ErrorPathNotFound       APIErrorCode = "PATH_NOT_FOUND"
	ErrorStorageConnection  APIErrorCode = "STORAGE_CONNECTION"
)

// ErrorResponse represents error response.
type ErrorResponse struct {
	Code  APIErrorCode `json:"code"`
	Error string       `json:"error"`
}

// SourceActionResponse is a per-source response.
type SourceActionResponse struct {
	Success bool `json:"success"`
}

// MultipleSourceActionResponse contains per-source responses for all sources targeted by API command.
type MultipleSourceActionResponse struct {
	Sources map[string]SourceActionResponse `json:"sources"`
}

// CreateRepositoryRequest contains request to create a repository in a given storage
type CreateRepositoryRequest struct {
	ConnectRepositoryRequest
	NewRepositoryOptions repo.NewRepositoryOptions `json:"options"`
}

// ConnectRepositoryRequest contains request to connect to a repository.
type ConnectRepositoryRequest struct {
	Storage  blob.ConnectionInfo `json:"storage"`
	Password string              `json:"password"`
	Token    string              `json:"token"` // when set, overrides Storage and Password
}

// SupportedAlgorithmsResponse returns the list of supported algorithms for repository creation.
type SupportedAlgorithmsResponse struct {
	DefaultHashAlgorithm       string   `json:"defaultHash"`
	DefaultEncryptionAlgorithm string   `json:"defaultEncryption"`
	DefaultSplitterAlgorithm   string   `json:"defaultSplitter"`
	HashAlgorithms             []string `json:"hash"`
	EncryptionAlgorithms       []string `json:"encryption"`
	SplitterAlgorithms         []string `json:"splitter"`
	CompressionAlgorithms      []string `json:"compression"`
}

// CreateSnapshotSourceRequest contains request to create snapshot source and optionally create first snapshot.
type CreateSnapshotSourceRequest struct {
	Path           string        `json:"path"`
	CreateSnapshot bool          `json:"createSnapshot"`
	InitialPolicy  policy.Policy `json:"initialPolicy"` // policy to set on the source when first created, ignored if already exists
}

// CreateSnapshotSourceResponse contains response of creating snapshot source.
type CreateSnapshotSourceResponse struct {
	Created         bool `json:"created"`     // whether the source was created (false==previously existed)
	SnapshotStarted bool `json:"snapshotted"` // whether snapshotting has been started
}

// Snapshot describes single snapshot entry.
type Snapshot struct {
	ID               manifest.ID          `json:"id"`
	Source           snapshot.SourceInfo  `json:"source"`
	Description      string               `json:"description"`
	StartTime        time.Time            `json:"startTime"`
	EndTime          time.Time            `json:"endTime"`
	IncompleteReason string               `json:"incomplete,omitempty"`
	Summary          *fs.DirectorySummary `json:"summary"`
	RootEntry        string               `json:"rootID"`
	RetentionReasons []string             `json:"retention"`
}

// SnapshotsResponse contains a list of snapshots.
type SnapshotsResponse struct {
	Snapshots []*Snapshot `json:"snapshots"`
}
