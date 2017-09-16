package repo

import (
	"log"
	"sync"

	"github.com/kopia/kopia/blob"
)

type blockSizeCache struct {
	storage blob.Storage

	mu        sync.Mutex
	cache     map[string]int64
	completed map[string]bool
	populated map[string]bool
	cancel    blob.CancelFunc
}

func (c *blockSizeCache) close() error {
	return nil
}

func (c *blockSizeCache) put(blockID string, size int64) {
	c.mu.Lock()
	c.cache[blockID] = size
	c.mu.Unlock()
}

func (c *blockSizeCache) getSize(blockID string) (int64, error) {
	c.mu.Lock()
	size, ok := c.cache[blockID]
	prefix := blockID[0:1]
	complete := c.completed[prefix]

	if !c.populated[prefix] {
		c.populated[prefix] = true
		go c.populate(prefix)

	}
	c.mu.Unlock()
	if ok {
		// found in cache, return
		return size, nil
	}

	if complete {
		// not found in cache and we loaded everything.
		return 0, blob.ErrBlockNotFound
	}

	s, err := c.storage.BlockSize(blockID)
	if err == nil {
		c.mu.Lock()
		c.cache[blockID] = s
		c.mu.Unlock()
	}

	return s, err
}

func (c *blockSizeCache) populate(prefix string) {
	ch, cancel := c.storage.ListBlocks(prefix)
	defer cancel()

	m := map[string]int64{}
	for b := range ch {
		if b.Error != nil {
			log.Printf("warning: got error populating block size cache with prefix %q: %v", prefix, b.Error)
			return
		}

		m[b.BlockID] = b.Length
	}

	c.mu.Lock()
	for k, v := range m {
		c.cache[k] = v
	}
	c.completed[prefix] = true
	c.mu.Unlock()
}

func newBlockSizeCache(s blob.Storage) *blockSizeCache {
	c := &blockSizeCache{
		storage:   s,
		cache:     map[string]int64{},
		completed: map[string]bool{},
		populated: map[string]bool{},
	}
	return c
}
