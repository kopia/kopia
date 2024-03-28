package repomodel

import (
	"context"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

// RepositorySession models the behavior of a single session in an repository.
type RepositorySession struct {
	OpenRepo *OpenRepository

	WrittenContents  *TrackingSet[content.ID]
	WrittenManifests *TrackingSet[manifest.ID]
}

// WriteContent adds the provided content ID to the model.
func (s *RepositorySession) WriteContent(ctx context.Context, cid content.ID) {
	s.WrittenContents.Add(ctx, cid)
}

// WriteManifest adds the provided manifest ID to the model.
func (s *RepositorySession) WriteManifest(ctx context.Context, mid manifest.ID) {
	s.WrittenManifests.Add(ctx, mid)
}

// Refresh refreshes the set of committed contents and manifest from repositor.
func (s *RepositorySession) Refresh(ctx context.Context, cids *TrackingSet[content.ID], mids *TrackingSet[manifest.ID]) {
	s.OpenRepo.Refresh(ctx, cids, mids)
}

// Flush flushes the changes written in this RepositorySession and makes them available
// to other RepositoryData model.
func (s *RepositorySession) Flush(ctx context.Context, wc *TrackingSet[content.ID], wm *TrackingSet[manifest.ID]) {
	s.OpenRepo.mu.Lock()
	defer s.OpenRepo.mu.Unlock()

	// data flushed is visible to other sessions in the same open repository.
	s.OpenRepo.ReadableContents.Add(ctx, wc.ids...)
	s.OpenRepo.ReadableManifests.Add(ctx, wm.ids...)

	// data flushed is visible to other sessions in other open repositories.
	s.OpenRepo.RepoData.CommittedContents.Add(ctx, wc.ids...)
	s.OpenRepo.RepoData.CommittedManifests.Add(ctx, wm.ids...)

	s.WrittenContents.RemoveAll(ctx, wc.ids...)
	s.WrittenManifests.RemoveAll(ctx, wm.ids...)
}
