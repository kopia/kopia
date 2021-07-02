package repomodel

import (
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

// RepositorySession models the behavior of a single session in an repository.
type RepositorySession struct {
	OpenRepo *OpenRepository

	WrittenContents  ContentSet
	WrittenManifests ManifestSet
}

// WriteContent adds the provided content ID to the model.
func (s *RepositorySession) WriteContent(cid content.ID) {
	s.WrittenContents.Add(cid)
}

// WriteManifest adds the provided manifest ID to the model.
func (s *RepositorySession) WriteManifest(mid manifest.ID) {
	s.WrittenManifests.Add(mid)
}

// Refresh refreshes the set of committed contents and manifest from repositor.
func (s *RepositorySession) Refresh() {
	s.OpenRepo.Refresh()
}

// Flush flushes the changes written in this RepositorySession and makes them available
// to other RepositoryData model.
func (s *RepositorySession) Flush(wc *ContentSet, wm *ManifestSet) {
	s.OpenRepo.mu.Lock()
	defer s.OpenRepo.mu.Unlock()

	// data flushed is visible to other sessions in the same open repository.
	s.OpenRepo.Contents.Add(wc.ids...)
	s.OpenRepo.Manifests.Add(wm.ids...)

	// data flushed is visible to other sessions in other open repositories.
	s.OpenRepo.RepoData.Contents.Add(wc.ids...)
	s.OpenRepo.RepoData.Manifests.Add(wm.ids...)

	s.WrittenContents.RemoveAll(wc.ids...)
	s.WrittenManifests.RemoveAll(wm.ids...)
}
