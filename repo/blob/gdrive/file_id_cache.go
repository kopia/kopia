package gdrive

import (
	"sync"

	"github.com/kopia/kopia/repo/blob"
)

const (
	changeLogCacheSize = 1 << 8
)

// FileIDCache is a cache for managing the association of blobID -> fileID.
type FileIDCache struct {
	// Map of blobID -> *CacheEntry.
	Blobs sync.Map
	// Record of recent cache changes.
	// It's stored as a synchronized circular buffer.
	ChangeLog [changeLogCacheSize]ChangeEntry
	// ChangeLogIdx indicates the next location to write to.
	// The log entry range is [ChangeLogIdx+1, ChangeLogIdx-1].
	ChangeLogIdx int
	// Guards access to ChangeLog.
	ChangeLogMut sync.RWMutex
}

// CacheEntry is a blob cache entry.
type CacheEntry struct {
	// Guards access to the entry. Must be taken before accessing fields.
	Mut sync.Mutex
	// blobID. Is always populated.
	BlobID blob.ID
	// Associated fileID. If empty, the blob doesn't have a cached fileID.
	FileID string
}

// ChangeEntry is a blob change entry.
type ChangeEntry struct {
	// blobID. If empty, the entry has not been written to yet.
	BlobID blob.ID
	// Associated fileID. If empty, the blob is deleted.
	FileID string
}

// Lookup finds the cache entry for blobID.
// The entry may be read or mutated.
// The callback is guaranteed to have exclusive access to the entry.
func (cache *FileIDCache) Lookup(blobID blob.ID, callback func(entry *CacheEntry) (interface{}, error)) (interface{}, error) {
	entry := cache.getEntry(blobID)

	entry.Mut.Lock()

	result, err := callback(entry)

	entry.Mut.Unlock()

	return result, err
}

// Internal method for retrieving an entry.
func (cache *FileIDCache) getEntry(blobID blob.ID) *CacheEntry {
	loaded, _ := cache.Blobs.LoadOrStore(blobID, &CacheEntry{
		Mut:    sync.Mutex{},
		BlobID: blobID,
		FileID: "",
	})

	return loaded.(*CacheEntry) //nolint:forcetypeassert
}

// BlindPut blindly mutates the association for a blobID.
func (cache *FileIDCache) BlindPut(blobID blob.ID, fileID string) {
	_, _ = cache.Lookup(blobID, func(entry *CacheEntry) (interface{}, error) {
		entry.FileID = fileID
		return nil, nil
	})
}

// RecordBlobChange records a newly created or deleted blob.
// An empty fileID signals that the blob is deleted.
func (cache *FileIDCache) RecordBlobChange(blobID blob.ID, fileID string) {
	cache.ChangeLogMut.Lock()

	i := cache.ChangeLogIdx
	cache.ChangeLog[i] = ChangeEntry{
		BlobID: blobID,
		FileID: fileID,
	}
	cache.ChangeLogIdx = circularBufferNext(i)

	cache.ChangeLogMut.Unlock()
}

// VisitBlobChanges iterates through newly created or deleted blobs.
func (cache *FileIDCache) VisitBlobChanges(callback func(blobID blob.ID, fileID string)) {
	cache.ChangeLogMut.RLock()

	for i := circularBufferNext(cache.ChangeLogIdx); i != cache.ChangeLogIdx; i = circularBufferNext(i) {
		entry := cache.ChangeLog[i]
		if entry.BlobID != "" {
			callback(entry.BlobID, entry.FileID)
		}
	}

	cache.ChangeLogMut.RUnlock()
}

// Clear resets the file ID cache.
func (cache *FileIDCache) Clear() {
	cache.Blobs = sync.Map{}
	cache.ChangeLog = [changeLogCacheSize]ChangeEntry{}
}

func circularBufferNext(curr int) int {
	if curr == changeLogCacheSize-1 {
		return 0
	}

	return curr + 1
}

// NewFileIDCache creates a new FileIDCache.
func NewFileIDCache() (*FileIDCache, error) {
	return &FileIDCache{
		Blobs:        sync.Map{},
		ChangeLog:    [changeLogCacheSize]ChangeEntry{},
		ChangeLogIdx: 0,
		ChangeLogMut: sync.RWMutex{},
	}, nil
}
