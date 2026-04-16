package cachefs

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
)

var log = logging.Module("kopia/cachefs")

const dirCacheExpiration = 24 * time.Hour

type cacheEntry struct {
	id   string
	prev *cacheEntry
	next *cacheEntry

	expireAfter time.Time
	entries     []fs.Entry
}

// Cache maintains in-memory cache of recently-read data to speed up filesystem operations.
type Cache struct {
	mu sync.Locker

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
type Loader func(ctx context.Context) ([]fs.Entry, error)

// EntryWrapper allows an fs.Entry to be modified before inserting into the cache.
type EntryWrapper func(entry fs.Entry) fs.Entry

// IterateEntries reads the contents of a provided directory using ObjectID of a directory (if any) to cache
// the results. The given callback is invoked on each item in the directory.
func (c *Cache) IterateEntries(ctx context.Context, d fs.Directory, w EntryWrapper, callback func(context.Context, fs.Entry) error) error {
	if h, ok := d.(object.HasObjectID); ok {
		cacheID := h.ObjectID().String()

		entries, err := c.getEntries(
			ctx,
			cacheID,
			dirCacheExpiration,
			func(innerCtx context.Context) ([]fs.Entry, error) {
				return fs.GetAllEntries(innerCtx, d)
			},
			w,
		)
		if err != nil {
			return err
		}

		for _, e := range entries {
			err = callback(ctx, e)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return fs.IterateEntries(ctx, d, callback) //nolint:wrapcheck
}

func (c *Cache) getEntriesFromCacheLocked(ctx context.Context, id string) []fs.Entry {
	if v, ok := c.data[id]; id != "" && ok {
		if clock.Now().Before(v.expireAfter) {
			c.moveToHead(v)

			if c.debug {
				log(ctx).Debugf("cache hit for %q (valid until %v)", id, v.expireAfter)
			}

			return v.entries
		}

		// time expired
		if c.debug {
			log(ctx).Debugf("removing expired cache entry %q after %v", id, v.expireAfter)
		}

		c.removeEntryLocked(v)
	}

	return nil
}

// getEntries consults the cache and either retrieves the contents of directory listing from the cache
// or invokes the provides callback and adds the results to cache.
func (c *Cache) getEntries(ctx context.Context, id string, expirationTime time.Duration, cb Loader, w EntryWrapper) ([]fs.Entry, error) {
	if c == nil {
		return cb(ctx)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entries := c.getEntriesFromCacheLocked(ctx, id); entries != nil {
		return entries, nil
	}

	if c.debug {
		log(ctx).Debugf("cache miss for %q", id)
	}

	raw, err := cb(ctx)
	if err != nil {
		return nil, err
	}

	wrapped := make([]fs.Entry, len(raw))
	for i, entry := range raw {
		wrapped[i] = w(entry)
	}

	if len(wrapped) > c.maxDirectoryEntries {
		// no point caching since it would not fit anyway, just return it.
		return wrapped, nil
	}

	entry := &cacheEntry{
		id:          id,
		entries:     wrapped,
		expireAfter: clock.Now().Add(expirationTime),
	}

	c.addToHead(entry)
	c.data[id] = entry

	c.totalDirectoryEntries += len(wrapped)
	for c.totalDirectoryEntries > c.maxDirectoryEntries || len(c.data) > c.maxDirectories {
		c.removeEntryLocked(c.tail)
	}

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

//nolint:gochecknoglobals
var defaultOptions = &Options{
	MaxCachedDirectories: 1000,   //nolint:mnd
	MaxCachedEntries:     100000, //nolint:mnd
}

// NewCache creates filesystem cache.
func NewCache(options *Options) *Cache {
	if options == nil {
		options = defaultOptions
	}

	return &Cache{
		mu:                  &sync.Mutex{},
		data:                make(map[string]*cacheEntry),
		maxDirectories:      options.MaxCachedDirectories,
		maxDirectoryEntries: options.MaxCachedEntries,
	}
}
