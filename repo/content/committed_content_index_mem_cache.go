package content

import (
	"bytes"
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type memoryCommittedContentIndexCache struct {
	mu       sync.Mutex
	contents map[blob.ID]packIndex
}

func (m *memoryCommittedContentIndexCache) hasIndexBlobID(ctx context.Context, indexBlobID blob.ID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.contents[indexBlobID] != nil, nil
}

func (m *memoryCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.contents[indexBlobID] = ndx

	return nil
}

func (m *memoryCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (packIndex, error) {
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

	n := map[blob.ID]packIndex{}

	for _, u := range used {
		if v, ok := m.contents[u]; ok {
			n[u] = v
		}
	}

	m.contents = n

	return nil
}
