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

func (m *mutexMap) exclusiveLock(key string) {
	if m != nil {
		m.getMutexAndAddRef(key).Lock()
	}
}

func (m *mutexMap) tryExclusiveLock(key string) bool {
	if m != nil {
		if !m.getMutexAndAddRef(key).TryLock() {
			m.getMutexAndReleaseRef(key)
			return false
		}

		return true
	}

	return true
}

func (m *mutexMap) exclusiveUnlock(key string) {
	if m != nil {
		m.getMutexAndReleaseRef(key).Unlock()
	}
}

func (m *mutexMap) sharedLock(key string) {
	if m != nil {
		m.getMutexAndAddRef(key).RLock()
	}
}

func (m *mutexMap) trySharedLock(key string) bool {
	if m != nil {
		if !m.getMutexAndAddRef(key).TryRLock() {
			m.getMutexAndReleaseRef(key)
			return false
		}

		return true
	}

	return true
}

func (m *mutexMap) sharedUnlock(key string) {
	if m != nil {
		m.getMutexAndReleaseRef(key).RUnlock()
	}
}

func (m *mutexMap) getMutexAndAddRef(key string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	ent := m.entries[key]
	if ent == nil {
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

	ent := m.entries[key]
	ent.refCount--

	if ent.refCount == 0 {
		delete(m.entries, key)
	}

	return ent.mut
}

func newMutexMap() *mutexMap {
	return &mutexMap{
		entries: make(map[string]*mutexMapEntry),
	}
}
