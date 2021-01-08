package manifest

import (
	"sync"

	"github.com/kopia/kopia/repo/content"
)

// committedManifestManager manages committed manifest entries stored in 'm' contents.
type committedManifestManager struct {
	b contentManager

	cmmu                sync.Mutex
	locked              bool
	initialized         bool
	committedEntries    map[ID]*manifestEntry
	committedContentIDs map[content.ID]bool
}

func newCommittedManager(b contentManager) *committedManifestManager {
	return &committedManifestManager{
		b:                   b,
		committedEntries:    map[ID]*manifestEntry{},
		committedContentIDs: map[content.ID]bool{},
	}
}
