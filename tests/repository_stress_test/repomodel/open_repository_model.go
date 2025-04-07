package repomodel

import (
	"context"
	"sync"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
)

var log = logging.Module("repomodel") // +checklocksignore

// OpenRepository models the behavior of an open repository.
type OpenRepository struct {
	mu sync.Mutex

	RepoData          *RepositoryData           // +checklocksignore
	ReadableContents  *TrackingSet[content.ID]  // +checklocksignore
	ReadableManifests *TrackingSet[manifest.ID] // +checklocksignore

	EnableMaintenance bool

	openID string
}

// Refresh refreshes the set of committed Contents and manifest from repositor.
func (o *OpenRepository) Refresh(ctx context.Context, cids *TrackingSet[content.ID], mids *TrackingSet[manifest.ID]) {
	o.ReadableContents.Replace(ctx, cids.ids)
	o.ReadableManifests.Replace(ctx, mids.ids)
}

// NewSession creates new model for a session to access a repository.
func (o *OpenRepository) NewSession(sessionID string) *RepositorySession {
	return &RepositorySession{
		OpenRepo:         o,
		WrittenContents:  NewChangeSet[content.ID](o.openID + "-written-" + sessionID),
		WrittenManifests: NewChangeSet[manifest.ID](o.openID + "-written-" + sessionID),
	}
}
