package repomodel

import (
	"math/rand"
	"sync"

	"github.com/kopia/kopia/repo/content"
)

// ContentSet represents a set of contents.
type ContentSet struct {
	mu  sync.Mutex
	ids []content.ID
}

// PickRandom picks one random content from the set or empty string.
func (s *ContentSet) PickRandom() content.ID {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ids) == 0 {
		return content.EmptyID
	}

	//nolint:gosec
	return s.ids[rand.Intn(len(s.ids))]
}

// Snapshot returns the snapshot of all IDs.
func (s *ContentSet) Snapshot() ContentSet {
	s.mu.Lock()
	defer s.mu.Unlock()

	return ContentSet{
		ids: append([]content.ID(nil), s.ids...),
	}
}

// Replace replaces all elements in the set.
func (s *ContentSet) Replace(ids []content.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = append([]content.ID(nil), ids...)
}

// Add adds the provided items to the set.
func (s *ContentSet) Add(d ...content.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = append(s.ids, d...)
}

// RemoveAll removes the provided items from the set.
func (s *ContentSet) RemoveAll(d ...content.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids = removeAllContentIDs(s.ids, d)
}

func removeAllContentIDs(a, b []content.ID) []content.ID {
	var result []content.ID

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
func (s *ContentSet) Clear() ContentSet {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.ids
	s.ids = nil

	return ContentSet{ids: old}
}
