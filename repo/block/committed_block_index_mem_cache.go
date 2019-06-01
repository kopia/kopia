package block

import (
	"bytes"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type memoryCommittedBlockIndexCache struct {
	mu     sync.Mutex
	blocks map[blob.ID]packIndex
}

func (m *memoryCommittedBlockIndexCache) hasIndexBlobID(indexBlobID blob.ID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.blocks[indexBlobID] != nil, nil
}

func (m *memoryCommittedBlockIndexCache) addBlockToCache(indexBlobID blob.ID, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.blocks[indexBlobID] = ndx
	return nil
}

func (m *memoryCommittedBlockIndexCache) openIndex(indexBlobID blob.ID) (packIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.blocks[indexBlobID]
	if v == nil {
		return nil, errors.Errorf("block not found in cache: %v", indexBlobID)
	}

	return v, nil
}

func (m *memoryCommittedBlockIndexCache) expireUnused(used []blob.ID) error {
	return nil
}
