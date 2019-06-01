package block

import (
	"bytes"
	"sync"

	"github.com/pkg/errors"
)

type memoryCommittedBlockIndexCache struct {
	mu     sync.Mutex
	blocks map[string]packIndex
}

func (m *memoryCommittedBlockIndexCache) hasIndexBlockID(indexBlockID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.blocks[indexBlockID] != nil, nil
}

func (m *memoryCommittedBlockIndexCache) addBlockToCache(indexBlockID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.blocks[indexBlockID] = ndx
	return nil
}

func (m *memoryCommittedBlockIndexCache) openIndex(indexBlockID string) (packIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.blocks[indexBlockID]
	if v == nil {
		return nil, errors.Errorf("block not found in cache: %v", indexBlockID)
	}

	return v, nil
}

func (m *memoryCommittedBlockIndexCache) expireUnused(used []string) error {
	return nil
}
