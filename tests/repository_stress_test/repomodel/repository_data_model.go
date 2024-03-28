// Package repomodel provides simplified model of repository operation.
package repomodel

import (
	"sync/atomic"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

// RepositoryData models the data stored in the repository.
type RepositoryData struct {
	CommittedContents  *TrackingSet[content.ID]
	CommittedManifests *TrackingSet[manifest.ID]

	openCounter *int32
}

// OpenRepository returns an OpenRepository model based on current snapshot of RepositoryData.
func (d *RepositoryData) OpenRepository(openID string) *OpenRepository {
	return &OpenRepository{
		RepoData: d,

		ReadableContents:  d.CommittedContents.Snapshot(openID + "-contents"),
		ReadableManifests: d.CommittedManifests.Snapshot(openID + "-manifests"),
		EnableMaintenance: atomic.AddInt32(d.openCounter, 1) == 1,

		openID: openID,
	}
}

// NewRepositoryData creates new RepositoryData model.
func NewRepositoryData() *RepositoryData {
	return &RepositoryData{
		openCounter:        new(int32),
		CommittedContents:  NewChangeSet[content.ID]("committed-contents"),
		CommittedManifests: NewChangeSet[manifest.ID]("committed-manifests"),
	}
}
