package gdrive

import (
	"sync"

	"github.com/kopia/kopia/repo/blob"
)

const (
	changeLogCacheSize = 1 << 8
)

// fileIDCache is a cache for managing the association of blobID -> fileID.
type fileIDCache struct {
	// Map of blobID -> *cacheEntry.
	Blobs sync.Map

	// Guards access to ChangeLog.
	mu sync.RWMutex
	// Record of recent cache changes.
	// It's stored as a synchronized circular buffer.
	// +checklocks:mu
	changeLog [changeLogCacheSize]changeEntry
	// changeLogIdx indicates the next location to write to.
	// The log entry range is [changeLogIdx+1, changeLogIdx-1].
	// +checklocks:mu
	changeLogIdx int
}

// cacheEntry is a blob cache entry.
type cacheEntry struct {
	// Guards access to the entry. Must be taken before accessing fields.
	Mut sync.Mutex
	// blobID. Is always populated.
	BlobID blob.ID
	// Associated fileID. If empty, the blob doesn't have a cached fileID.
	FileID string
}

// changeEntry is a blob change entry.
type changeEntry struct {
	// blobID. If empty, the entry has not been written to yet.
	BlobID blob.ID
	// Associated fileID. If empty, the blob is deleted.
	FileID string
}

// Lookup finds the cache entry for blobID.
// The entry may be read or mutated.
// The callback is guaranteed to have exclusive access to the entry.
func (cache *fileIDCache) Lookup(blobID blob.ID, callback func(entry *cacheEntry) (any, error)) (any, error) {
	entry := cache.getEntry(blobID)

	entry.Mut.Lock()

	result, err := callback(entry)

	entry.Mut.Unlock()

	return result, err
}

// Internal method for retrieving an entry.
func (cache *fileIDCache) getEntry(blobID blob.ID) *cacheEntry {
	loaded, _ := cache.Blobs.LoadOrStore(blobID, &cacheEntry{
		Mut:    sync.Mutex{},
		BlobID: blobID,
		FileID: "",
	})

	return loaded.(*cacheEntry) //nolint:forcetypeassert
}

// BlindPut blindly mutates the association for a blobID.
func (cache *fileIDCache) BlindPut(blobID blob.ID, fileID string) {
	_, _ = cache.Lookup(blobID, func(entry *cacheEntry) (any, error) {
		entry.FileID = fileID
		return nil, nil
	})
}

// RecordBlobChange records a newly created or deleted blob.
// An empty fileID signals that the blob is deleted.
func (cache *fileIDCache) RecordBlobChange(blobID blob.ID, fileID string) {
	cache.mu.Lock()

	i := cache.changeLogIdx
	cache.changeLog[i] = changeEntry{
		BlobID: blobID,
		FileID: fileID,
	}
	cache.changeLogIdx = circularBufferNext(i)

	cache.mu.Unlock()
}

// VisitBlobChanges iterates through newly created or deleted blobs.
func (cache *fileIDCache) VisitBlobChanges(callback func(blobID blob.ID, fileID string)) {
	cache.mu.RLock()

	for i := circularBufferNext(cache.changeLogIdx); i != cache.changeLogIdx; i = circularBufferNext(i) {
		entry := cache.changeLog[i]
		if entry.BlobID != "" {
			callback(entry.BlobID, entry.FileID)
		}
	}

	cache.mu.RUnlock()
}

// Clear resets the file ID cache.
func (cache *fileIDCache) Clear() {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.Blobs = sync.Map{}
	cache.changeLog = [changeLogCacheSize]changeEntry{}
}

func circularBufferNext(curr int) int {
	if curr == changeLogCacheSize-1 {
		return 0
	}

	return curr + 1
}

// newFileIDCache creates a new fileIDCache.
func newFileIDCache() *fileIDCache {
	return &fileIDCache{
		Blobs:        sync.Map{},
		changeLog:    [changeLogCacheSize]changeEntry{},
		changeLogIdx: 0,
		mu:           sync.RWMutex{},
	}
}
