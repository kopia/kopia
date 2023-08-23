package cache

import (
	"sync"
)

// manages a map of RWMutexes indexed by string keys
// mutexes are allocated on demand and released when no longer needed.
type mutexMap struct {
	mu sync.Mutex

	// +checklocks:mu
	entries map[string]*mutexMapEntry
}

type mutexMapEntry struct {
	mut      *sync.RWMutex
	refCount int
}

// +checklocksignore.
func (m *mutexMap) exclusiveLock(key string) {
	m.getMutexAndAddRef(key).Lock()
}

func (m *mutexMap) tryExclusiveLock(key string) bool {
	if !m.getMutexAndAddRef(key).TryLock() { // +checklocksignore
		m.getMutexAndReleaseRef(key)
		return false
	}

	return true
}

func (m *mutexMap) exclusiveUnlock(key string) {
	m.getMutexAndReleaseRef(key).Unlock() // +checklocksignore
}

// +checklocksignore.
func (m *mutexMap) sharedLock(key string) {
	m.getMutexAndAddRef(key).RLock()
}

func (m *mutexMap) trySharedLock(key string) bool {
	if !m.getMutexAndAddRef(key).TryRLock() { // +checklocksignore
		m.getMutexAndReleaseRef(key)
		return false
	}

	return true
}

func (m *mutexMap) sharedUnlock(key string) {
	m.getMutexAndReleaseRef(key).RUnlock() // +checklocksignore
}

func (m *mutexMap) getMutexAndAddRef(key string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	ent := m.entries[key]
	if ent == nil {
		if m.entries == nil {
			m.entries = make(map[string]*mutexMapEntry)
		}

		ent = &mutexMapEntry{
			mut: &sync.RWMutex{},
		}

		m.entries[key] = ent
	}

	ent.refCount++

	return ent.mut
}

func (m *mutexMap) getMutexAndReleaseRef(key string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.entries == nil {
		panic("attempted to call unlock without a lock")
	}

	ent := m.entries[key]
	ent.refCount--

	if ent.refCount == 0 {
		delete(m.entries, key)
	}

	return ent.mut
}
