package repomodel

import (
	"math/rand"
	"sync"

	"github.com/kopia/kopia/repo/manifest"
)

// ManifestSet represents a set of manifests.
type ManifestSet struct {
	mu  sync.Mutex
	ids []manifest.ID
}

// PickRandom picks one random manifest from the set or empty string.
func (s *ManifestSet) PickRandom() manifest.ID {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ids) == 0 {
		return ""
	}

	//nolint:gosec
	return s.ids[rand.Intn(len(s.ids))]
}

// Snapshot returns the snapshot of all IDs.
func (s *ManifestSet) Snapshot() ManifestSet {
	s.mu.Lock()
	defer s.mu.Unlock()

	return ManifestSet{
		ids: append([]manifest.ID(nil), s.ids...),
	}
}

// Replace replaces all elements in the set.
func (s *ManifestSet) Replace(ids []manifest.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = append([]manifest.ID(nil), ids...)
}

// Add adds the provided items to the set.
func (s *ManifestSet) Add(d ...manifest.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = append(s.ids, d...)
}

// RemoveAll removes the provided items from the set.
func (s *ManifestSet) RemoveAll(d ...manifest.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = removeAllManifestIDs(s.ids, d)
}

func removeAllManifestIDs(a, b []manifest.ID) []manifest.ID {
	var result []manifest.ID

	for _, v := range a {
		found := false

		for _, v2 := range b {
			if v2 == v {
				found = true
				break
			}
		}

		if !found {
			result = append(result, v)
		}
	}

	return result
}

// Clear removes all elements from the set.
func (s *ManifestSet) Clear() ManifestSet {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.ids
	s.ids = nil

	return ManifestSet{ids: old}
}
