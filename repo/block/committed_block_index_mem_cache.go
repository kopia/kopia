package block

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
)

type memoryCommittedBlockIndexCache struct {
	mu     sync.Mutex
	blocks map[string]packindex.Index
}

func (m *memoryCommittedBlockIndexCache) hasIndexBlockID(indexBlockID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.blocks[indexBlockID] != nil, nil
}

func (m *memoryCommittedBlockIndexCache) addBlockToCache(indexBlockID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ndx, err := packindex.Open(bytes.NewReader(data))
	if err != nil {
		return err
	}

	m.blocks[indexBlockID] = ndx
	return nil
}

func (m *memoryCommittedBlockIndexCache) openIndex(indexBlockID string) (packindex.Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.blocks[indexBlockID]
	if v == nil {
		return nil, fmt.Errorf("block not found in cache: %v", indexBlockID)
	}

	return v, nil
}

func (m *memoryCommittedBlockIndexCache) expireUnused(used []string) error {
	return nil
}
