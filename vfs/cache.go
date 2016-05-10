package vfs

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"unsafe"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

type dirCache struct {
	sync.Mutex
	totalEntries        int
	totalEntriesSize    int
	maxEntries          int
	maxTotalEntriesSize int
	entries             map[repo.ObjectID]*dirCacheEntry
	sorted              *list.List
}

type dirCacheEntry struct {
	oid       repo.ObjectID
	dir       fs.Directory
	listEntry *list.Element
	totalSize int
}

func (dce *dirCacheEntry) String() string {
	return fmt.Sprintf("dir: %v items", len(dce.dir))
}

func (dc *dirCache) Add(oid repo.ObjectID, d fs.Directory) {
	dc.Lock()
	defer dc.Unlock()

	if _, ok := dc.entries[oid]; ok {
		// Already present, no-op
		return
	}

	dirSize := 0
	for _, e := range d {
		entrySize := int(unsafe.Sizeof(e)) + len(e.ObjectID)

		dirSize += entrySize
	}

	// Create entry, add to both map and list
	e := &dirCacheEntry{
		oid:       oid,
		dir:       d,
		totalSize: dirSize,
	}
	e.listEntry = dc.sorted.PushFront(e)
	dc.entries[oid] = e
	dc.totalEntries += len(d)
	dc.totalEntriesSize += e.totalSize
	log.Printf("Added %v to cache (%v entries). Total entries %v size %v", oid, len(d), dc.totalEntries, dc.totalEntriesSize)

	for dc.totalEntries > 0 && (dc.totalEntries > dc.maxEntries || dc.totalEntriesSize > dc.maxTotalEntriesSize) {
		dc.removeLast()
	}
}

func (dc *dirCache) Get(oid repo.ObjectID) fs.Directory {
	dc.Lock()
	defer dc.Unlock()

	if e, ok := dc.entries[oid]; ok {
		if e.listEntry.Prev() != nil {
			dc.sorted.MoveToFront(e.listEntry)
		}

		return e.dir
	}

	return nil
}

func (dc *dirCache) removeLast() {
	last := dc.sorted.Back()
	dce := last.Value.(*dirCacheEntry)
	dc.totalEntries -= len(dce.dir)
	dc.totalEntriesSize -= dce.totalSize
	dc.sorted.Remove(last)
	delete(dc.entries, dce.oid)
	log.Printf("Evicted %v from cache. Total entries %v size %v", dce.oid, dc.totalEntries, dc.totalEntriesSize)
}

func (dc *dirCache) dump() {
	log.Printf("cache entries %v max: %v", dc.totalEntries, dc.maxEntries)
	for k, v := range dc.entries {
		log.Printf("entries[%v] = %v", k, v)
	}
}

func newDirCache(maxEntries int, maxTotalEntriesSize int) *dirCache {
	return &dirCache{
		maxEntries:          maxEntries,
		maxTotalEntriesSize: maxTotalEntriesSize,
		entries:             map[repo.ObjectID]*dirCacheEntry{},
		sorted:              list.New(),
	}
}
