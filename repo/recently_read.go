package repo

import (
	"sync"

	"github.com/kopia/kopia/repo/content"
)

type recentlyRead struct {
	mu sync.Mutex
	// +checklocks:mu
	contentList []content.ID
	// +checklocks:mu
	next int
	// +checklocks:mu
	contentSet map[content.ID]struct{}
}

func (r *recentlyRead) add(contentID content.ID) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.contentSet == nil {
		r.contentList = make([]content.ID, numRecentReadsToCache)
		r.contentSet = make(map[content.ID]struct{})
	}

	delete(r.contentSet, r.contentList[r.next])
	r.contentList[r.next] = contentID
	r.contentSet[contentID] = struct{}{}
	r.next = (r.next + 1) % len(r.contentList)
}

func (r *recentlyRead) exists(contentID content.ID) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.contentSet[contentID]

	return ok
}
