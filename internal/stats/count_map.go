package stats

import (
	"sync"
	"sync/atomic"
)

// CountersMap is a concurrency-safe map from keys of type K to uint32 counters.
// It allows increments and retrievals of counts from concurrent go routines
// without additional concurrency coordination.
// Added counters cannot be removed.
type CountersMap[K comparable] struct {
	// Stores map[K]*atomic.Uint32
	// The counter is stored as pointer so it can be updated with atomic operations.
	data   sync.Map
	length atomic.Uint32
}

// Increment increases the counter for the specified key by 1.
// Returns true if the key already existed, false if it was newly created.
func (m *CountersMap[K]) Increment(key K) bool {
	return m.add(key, 1)
}

// add increases the counter for the specified key by the value of v.
// Returns true if the key already existed, false if it was newly created.
// Note: if this function is directly exported at some point, then overflow
// checks should be performed.
func (m *CountersMap[K]) add(key K, v uint32) bool {
	// Attempt looking for an already existing entry first to avoid spurious
	// (value) allocations in workloads where the entry likely exists already.
	actual, found := m.data.Load(key)
	if !found {
		actual, found = m.data.LoadOrStore(key, &atomic.Uint32{})
		if !found {
			m.length.Add(1)
		}
	}

	actual.(*atomic.Uint32).Add(v) //nolint:forcetypeassert

	return found
}

// Length returns the approximate number of keys in the map. The actual number
// of keys can be equal or larger to the returned value, but not less.
func (m *CountersMap[K]) Length() uint {
	return uint(m.length.Load())
}

// Get returns the current value of the counter for key, or 0 when key does not exist.
func (m *CountersMap[K]) Get(key K) (uint32, bool) {
	actual, ok := m.data.Load(key)
	if !ok {
		return 0, false // Key not found, return 0
	}

	return actual.(*atomic.Uint32).Load(), true //nolint:forcetypeassert
}

// Range iterates over all key/count pairs in the map, calling f for each item.
// If f returns false, iteration stops.
func (m *CountersMap[K]) Range(f func(key K, count uint32) bool) {
	m.data.Range(func(k any, v any) bool {
		return f(k.(K), v.(*atomic.Uint32).Load()) //nolint:forcetypeassert
	})
}

// CountMap returns the current value of the counters. The counters do not
// correspond to a consistent snapshot of the map, the counters may change
// while the returned map is built.
func (m *CountersMap[K]) CountMap() map[K]uint32 {
	r := map[K]uint32{}

	m.Range(func(key K, count uint32) bool {
		r[key] = count

		return true
	})

	return r
}
