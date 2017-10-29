package metadata

import (
	"crypto/rand"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/kopia/kopia/storage"
)

const (
	// MetadataBlockPrefix is a prefix used for metadata blocks in repository storage.
	MetadataBlockPrefix = "VLT"
)

type metadataCache struct {
	st storage.Storage

	mu            sync.Mutex
	sortedNames   []string
	nameToCacheID map[string]string
	cachedData    map[string][]byte
}

func (c *metadataCache) ListBlocks(prefix string) ([]string, error) {
	var result []string

	c.mu.Lock()
	p := sort.SearchStrings(c.sortedNames, prefix)
	for p < len(c.sortedNames) && strings.HasPrefix(c.sortedNames[p], prefix) {
		if !isReservedName(c.sortedNames[p]) {
			result = append(result, c.sortedNames[p])
		}
		p++
	}
	c.mu.Unlock()
	return result, nil
}

func (c *metadataCache) GetBlock(name string) ([]byte, error) {
	c.mu.Lock()
	cid := c.nameToCacheID[name]
	if cid == "" {
		c.mu.Unlock()
		return nil, storage.ErrBlockNotFound
	}

	// see if the data is cached
	if data, ok := c.cachedData[cid]; ok {
		c.mu.Unlock()
		return cloneBytes(data), nil
	}

	c.mu.Unlock()

	// not cached, fetch from the storage
	b, err := c.st.GetBlock(MetadataBlockPrefix+name, 0, -1)
	if err != nil {
		return nil, err
	}

	// now race to add to cache, does not matter who wins.
	c.mu.Lock()
	c.cachedData[cid] = b
	c.mu.Unlock()
	return cloneBytes(b), nil
}

func (c *metadataCache) PutBlock(name string, data []byte) error {
	if err := c.st.PutBlock(MetadataBlockPrefix+name, data); err != nil {
		return err
	}

	b := make([]byte, 8)
	io.ReadFull(rand.Reader, b)

	c.mu.Lock()
	cid := fmt.Sprintf("%v-new-%x", name, b)
	c.nameToCacheID[name] = cid
	p := sort.SearchStrings(c.sortedNames, name)
	if p >= len(c.sortedNames) || c.sortedNames[p] != name {
		// Name not present, p is the index where p should be inserted.
		c.sortedNames = append(append(
			append([]string(nil), c.sortedNames[0:p]...),
			name),
			c.sortedNames[p:]...)
	}

	c.mu.Unlock()

	return nil
}

func (c *metadataCache) DeleteBlock(name string) error {
	c.mu.Lock()
	if cid := c.nameToCacheID[name]; cid != "" {
		delete(c.nameToCacheID, name)
		delete(c.cachedData, cid)

		// Delete from sortedNames
		p := sort.SearchStrings(c.sortedNames, name)
		if p < len(c.sortedNames) && c.sortedNames[p] == name {
			// Found at index 'p' build a new slice from [0..p), [p+1,...)
			newSlice := c.sortedNames[0:p]
			for _, n := range c.sortedNames[p+1:] {
				newSlice = append(newSlice, n)
			}
			c.sortedNames = newSlice
		}
	}
	c.mu.Unlock()

	return c.st.DeleteBlock(MetadataBlockPrefix + name)
}

// refresh refreshes the list of blocks in the cache, but does not load or expire previously cached.
func (c *metadataCache) refresh() error {
	var sortedNames []string
	nameToCacheID := map[string]string{}

	ch, cancel := c.st.ListBlocks(MetadataBlockPrefix)
	defer cancel()
	for it := range ch {
		if it.Error != nil {
			return it.Error
		}

		n := strings.TrimPrefix(it.BlockID, MetadataBlockPrefix)
		sortedNames = append(sortedNames, n)
		nameToCacheID[n] = fmt.Sprintf("%v-%v-%v", it.BlockID, it.Length, it.TimeStamp.UnixNano())
	}

	c.setLoaded(sortedNames, nameToCacheID)
	return nil
}

func (c *metadataCache) setLoaded(sortedNames []string, nameToCacheID map[string]string) {
	c.mu.Lock()
	c.sortedNames = sortedNames
	c.nameToCacheID = nameToCacheID
	c.mu.Unlock()
}

func cloneBytes(d []byte) []byte {
	return append([]byte(nil), d...)
}

func newMetadataCache(st storage.Storage) (*metadataCache, error) {
	c := &metadataCache{
		st:            st,
		nameToCacheID: make(map[string]string),
		cachedData:    make(map[string][]byte),
	}
	if err := c.refresh(); err != nil {
		return nil, err
	}

	return c, nil
}
