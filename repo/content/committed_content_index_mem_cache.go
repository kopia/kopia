package content

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

type memoryCommittedContentIndexCache struct {
	mu sync.Mutex

	// +checklocks:mu
	contents map[blob.ID]index.Index

	v1PerContentOverhead func() int // +checklocksignore
}

func (m *memoryCommittedContentIndexCache) hasIndexBlobID(ctx context.Context, indexBlobID blob.ID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.contents[indexBlobID] != nil, nil
}

func (m *memoryCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data gather.Bytes) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := index.Open(data.ToByteSlice(), nil, m.v1PerContentOverhead)
	if err != nil {
		return errors.Wrapf(err, "error opening index blob %v", indexBlobID)
	}

	m.contents[indexBlobID] = ndx

	return nil
}

func (m *memoryCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (index.Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.contents[indexBlobID]
	if v == nil {
		return nil, errors.Errorf("content not found in cache: %v", indexBlobID)
	}

	return v, nil
}

func (m *memoryCommittedContentIndexCache) expireUnused(ctx context.Context, used []blob.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := map[blob.ID]index.Index{}

	for _, u := range used {
		if v, ok := m.contents[u]; ok {
			n[u] = v
		}
	}

	m.contents = n

	return nil
}
