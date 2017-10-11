package cachefs

import (
	"log"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/object"
)

type cacheEntry struct {
	id   string
	prev *cacheEntry
	next *cacheEntry

	expireAfter time.Time
	entries     fs.Entries
}

// Cache maintains in-memory cache of recently-read data to speed up filesystem operations.
type Cache struct {
	mu sync.Mutex

	totalDirectoryEntries int
	maxDirectories        int
	maxDirectoryEntries   int
	data                  map[string]*cacheEntry

	// Doubly-linked list of entries, in access time order
	head *cacheEntry
	tail *cacheEntry

	debug bool
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

// Loader provides data to be stored in the cache.
type Loader func() (fs.Entries, error)

// Readdir reads the contents of a provided directory using ObjectID of a directory (if any) to cache
// the results.
func (c *Cache) Readdir(d fs.Directory) (fs.Entries, error) {
	if h, ok := d.(object.HasObjectID); ok {
		cacheID := h.ObjectID().String()
		cacheExpiration := 24 * time.Hour
		return c.getEntries(cacheID, cacheExpiration, d.Readdir)
	}

	return d.Readdir()
}

// getEntries consults the cache and either retrieves the contents of directory listing from the cache
// or invokes the provides callback and adds the results to cache.
func (c *Cache) getEntries(id string, expirationTime time.Duration, cb Loader) (fs.Entries, error) {
	if c == nil {
		return cb()
	}

	c.mu.Lock()
	if v, ok := c.data[id]; id != "" && ok {
		if time.Now().Before(v.expireAfter) {
			c.moveToHead(v)
			c.mu.Unlock()
			if c.debug {
				log.Printf("cache hit for %q (valid until %v)", id, v.expireAfter)
			}
			return v.entries, nil
		}

		// time expired
		if c.debug {
			log.Printf("removing expired cache entry %q after %v", id, v.expireAfter)
		}
		c.removeEntryLocked(v)
	}

	if c.debug {
		log.Printf("cache miss for %q", id)
	}
	raw, err := cb()
	if err != nil {
		return nil, err
	}

	if len(raw) > c.maxDirectoryEntries {
		// no point caching since it would not fit anyway, just return it.
		c.mu.Unlock()
		return raw, nil
	}

	entry := &cacheEntry{
		id:          id,
		entries:     raw,
		expireAfter: time.Now().Add(expirationTime),
	}
	c.addToHead(entry)
	c.data[id] = entry

	c.totalDirectoryEntries += len(raw)
	for c.totalDirectoryEntries > c.maxDirectoryEntries || len(c.data) > c.maxDirectories {
		c.removeEntryLocked(c.tail)
	}

	c.mu.Unlock()

	return raw, nil
}

func (c *Cache) removeEntryLocked(toremove *cacheEntry) {
	c.remove(toremove)
	c.totalDirectoryEntries -= len(toremove.entries)
	delete(c.data, toremove.id)
}

// Options specifies behavior of filesystem Cache.
type Options struct {
	MaxCachedDirectories int
	MaxCachedEntries     int
}

var defaultOptions = &Options{
	MaxCachedDirectories: 1000,
	MaxCachedEntries:     100000,
}

// NewCache creates filesystem cache.
func NewCache(options *Options) *Cache {
	if options == nil {
		options = defaultOptions
	}

	return &Cache{
		data:                make(map[string]*cacheEntry),
		maxDirectories:      options.MaxCachedDirectories,
		maxDirectoryEntries: options.MaxCachedEntries,
	}
}
