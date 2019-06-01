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

func (m *memoryCommittedBlockIndexCache) hasIndexBlockID(indexBlockID blob.ID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.blocks[indexBlockID] != nil, nil
}

func (m *memoryCommittedBlockIndexCache) addBlockToCache(indexBlockID blob.ID, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.blocks[indexBlockID] = ndx
	return nil
}

func (m *memoryCommittedBlockIndexCache) openIndex(indexBlockID blob.ID) (packIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.blocks[indexBlockID]
	if v == nil {
		return nil, errors.Errorf("block not found in cache: %v", indexBlockID)
	}

	return v, nil
}

func (m *memoryCommittedBlockIndexCache) expireUnused(used []blob.ID) error {
	return nil
}
