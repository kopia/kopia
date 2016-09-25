// +build !windows

// Package fuse implements FUSE filesystem nodes for mounting contents of filesystem stored in repository.
//
// The FUSE implementation used is from bazil.org/fuse
package fuse

import (
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/fs"
)

type cacheEntry struct {
	id   int64
	prev *cacheEntry
	next *cacheEntry

	entries fs.Entries
}

// Cache maintains in-memory cache of recently-read data to speed up filesystem operations.
type Cache struct {
	sync.Mutex

	nextID                int64
	totalDirectoryEntries int
	maxDirectories        int
	maxDirectoryEntries   int
	data                  map[int64]*cacheEntry

	// Doubly-linked list of entries, in access time order
	head *cacheEntry
	tail *cacheEntry
}

func (c *Cache) allocateID() int64 {
	if c == nil {
		return 0
	}

	return atomic.AddInt64(&c.nextID, 1)
}

func (c *Cache) moveToHead(e *cacheEntry) {
	if e == c.head {
		// Already at head, no change.
		return
	}

	c.remove(e)
	c.addToHead(e)
}

func (c *Cache) addToHead(e *cacheEntry) {
	if c.head != nil {
		e.next = c.head
		c.head.prev = e
		c.head = e
	} else {
		c.head = e
		c.tail = e
	}
}

func (c *Cache) remove(e *cacheEntry) {
	if e.prev == nil {
		// First element.
		c.head = e.next
	} else {
		e.prev.next = e.next
	}

	if e.next == nil {
		// Last element
		c.tail = e.prev
	} else {
		e.next.prev = e.prev
	}
}

func (c *Cache) getEntries(id int64, cb func() (fs.Entries, error)) (fs.Entries, error) {
	if c == nil {
		return cb()
	}

	c.Lock()
	if v, ok := c.data[id]; ok {
		c.moveToHead(v)
		c.Unlock()
		return v.entries, nil
	}

	raw, err := cb()
	if err != nil {
		return nil, err
	}

	if len(raw) > c.maxDirectoryEntries {
		// no point caching since it would not fit anyway, just return it.
		c.Unlock()
		return raw, nil
	}

	entry := &cacheEntry{
		id:      id,
		entries: raw,
	}
	c.addToHead(entry)
	c.data[id] = entry

	c.totalDirectoryEntries += len(raw)
	for c.totalDirectoryEntries > c.maxDirectoryEntries || len(c.data) > c.maxDirectories {
		toremove := c.tail
		c.remove(toremove)
		c.totalDirectoryEntries -= len(toremove.entries)
		delete(c.data, toremove.id)
	}

	c.Unlock()

	return raw, nil
}

// CacheOption modifies the behavior of FUSE node cache.
type CacheOption func(c *Cache)

// MaxCachedDirectories configures cache to allow at most the given number of cached directories.
func MaxCachedDirectories(count int) CacheOption {
	return func(c *Cache) {
		c.maxDirectories = count
	}
}

// MaxCachedDirectoryEntries configures cache to allow at most the given number entries in cached directories.
func MaxCachedDirectoryEntries(count int) CacheOption {
	return func(c *Cache) {
		c.maxDirectoryEntries = count
	}
}

// NewCache creates FUSE node cache.
func NewCache(options ...CacheOption) *Cache {
	c := &Cache{
		data:                make(map[int64]*cacheEntry),
		maxDirectories:      1000,
		maxDirectoryEntries: 100000,
	}

	for _, o := range options {
		o(c)
	}

	return c
}
