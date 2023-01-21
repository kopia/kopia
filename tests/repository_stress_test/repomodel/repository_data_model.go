// Package repomodel provides simplified model of repository operation.
package repomodel

import "sync/atomic"

// RepositoryData models the d stored in the repository.
type RepositoryData struct {
	Contents  ContentSet
	Manifests ManifestSet

	openCounter *int32
}

// OpenRepository returns an OpenRepository model based on current snapshot of RepositoryData.
func (d *RepositoryData) OpenRepository() *OpenRepository {
	return &OpenRepository{
		RepoData: d,

		Contents:          d.Contents.Snapshot(),
		Manifests:         d.Manifests.Snapshot(),
		EnableMaintenance: atomic.AddInt32(d.openCounter, 1) == 1,
	}
}

// NewRepositoryData creates new RepositoryData model.
func NewRepositoryData() *RepositoryData {
	return &RepositoryData{
		openCounter: new(int32),
	}
}
