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

func (mc *metadataCache) ListBlocks(prefix string) ([]string, error) {
	var result []string

	mc.mu.Lock()
	p := sort.SearchStrings(mc.sortedNames, prefix)
	for p < len(mc.sortedNames) && strings.HasPrefix(mc.sortedNames[p], prefix) {
		if !isReservedName(mc.sortedNames[p]) {
			result = append(result, mc.sortedNames[p])
		}
		p++
	}
	mc.mu.Unlock()
	return result, nil
}

func (mc *metadataCache) GetBlock(name string) ([]byte, error) {
	mc.mu.Lock()
	cid := mc.nameToCacheID[name]
	if cid == "" {
		mc.mu.Unlock()
		return nil, storage.ErrBlockNotFound
	}

	// see if the data is cached
	if data, ok := mc.cachedData[cid]; ok {
		mc.mu.Unlock()
		return cloneBytes(data), nil
	}

	mc.mu.Unlock()

	// not cached, fetch from the storage
	b, err := mc.st.GetBlock(MetadataBlockPrefix+name, 0, -1)
	if err != nil {
		return nil, err
	}

	// now race to add to cache, does not matter who wins.
	mc.mu.Lock()
	mc.cachedData[cid] = b
	mc.mu.Unlock()
	return cloneBytes(b), nil
}

func (mc *metadataCache) PutBlock(name string, data []byte) error {
	if err := mc.st.PutBlock(MetadataBlockPrefix+name, data); err != nil {
		return err
	}

	b := make([]byte, 8)
	io.ReadFull(rand.Reader, b)

	mc.mu.Lock()
	cid := fmt.Sprintf("%v-new-%x", name, b)
	mc.nameToCacheID[name] = cid
	p := sort.SearchStrings(mc.sortedNames, name)
	if p >= len(mc.sortedNames) || mc.sortedNames[p] != name {
		// Name not present, p is the index where p should be inserted.
		mc.sortedNames = append(append(
			append([]string(nil), mc.sortedNames[0:p]...),
			name),
			mc.sortedNames[p:]...)
	}

	mc.mu.Unlock()

	return nil
}

func (mc *metadataCache) DeleteBlock(name string) error {
	mc.mu.Lock()
	if cid := mc.nameToCacheID[name]; cid != "" {
		delete(mc.nameToCacheID, name)
		delete(mc.cachedData, cid)

		// Delete from sortedNames
		p := sort.SearchStrings(mc.sortedNames, name)
		if p < len(mc.sortedNames) && mc.sortedNames[p] == name {
			// Found at index 'p' build a new slice from [0..p), [p+1,...)
			newSlice := mc.sortedNames[0:p]
			for _, n := range mc.sortedNames[p+1:] {
				newSlice = append(newSlice, n)
			}
			mc.sortedNames = newSlice
		}
	}
	mc.mu.Unlock()

	return mc.st.DeleteBlock(MetadataBlockPrefix + name)
}

// refresh refreshes the list of blocks in the cache, but does not load or expire previously cached.
func (mc *metadataCache) refresh() error {
	var sortedNames []string
	nameToCacheID := map[string]string{}

	ch, cancel := mc.st.ListBlocks(MetadataBlockPrefix)
	defer cancel()
	for it := range ch {
		if it.Error != nil {
			return it.Error
		}

		n := strings.TrimPrefix(it.BlockID, MetadataBlockPrefix)
		sortedNames = append(sortedNames, n)
		nameToCacheID[n] = fmt.Sprintf("%v-%v-%v", it.BlockID, it.Length, it.TimeStamp.UnixNano())
	}

	mc.setLoaded(sortedNames, nameToCacheID)
	return nil
}

func (mc *metadataCache) setLoaded(sortedNames []string, nameToCacheID map[string]string) {
	mc.mu.Lock()
	mc.sortedNames = sortedNames
	mc.nameToCacheID = nameToCacheID
	mc.mu.Unlock()
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
