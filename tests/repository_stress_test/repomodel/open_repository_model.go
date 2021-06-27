package repomodel

import "sync"

// OpenRepository models the behavior of an open repository.
type OpenRepository struct {
	RepoData *RepositoryData

	Contents  ContentSet
	Manifests ManifestSet

	EnableMaintenance bool

	mu sync.Mutex
}

// Refresh refreshes the set of committed Contents and manifest from repositor.
func (o *OpenRepository) Refresh() {
	o.Contents.Replace(o.RepoData.Contents.Snapshot().ids)
	o.Manifests.Replace(o.RepoData.Manifests.Snapshot().ids)
}

// NewSession creates new model for a session to access a repository.
func (o *OpenRepository) NewSession() *RepositorySession {
	return &RepositorySession{
		OpenRepo: o,
	}
}
