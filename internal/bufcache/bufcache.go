// Package bufcache allocates and recycles byte slices used as buffers.
package bufcache

import (
	"sync"
)

type poolWithCapacity struct {
	capacity int
	pool     *sync.Pool
}

// pools keep track of sync.Pools holding pointers to byte slices of exactly the provided capacity.
// when allocating, we pick from the smallest pool that fits.
var pools = []poolWithCapacity{
	{1 << 8, &sync.Pool{}},  // 256 B
	{1 << 10, &sync.Pool{}}, // 1 KB
	{1 << 12, &sync.Pool{}}, // 4 KB
	{1 << 14, &sync.Pool{}}, // 16 KB
	{1 << 16, &sync.Pool{}}, // 64 KB
	{1 << 18, &sync.Pool{}}, // 256 KB
	{1 << 20, &sync.Pool{}}, // 1 MB
	{1 << 21, &sync.Pool{}}, // 2 MB
	{1 << 22, &sync.Pool{}}, // 4 MB
	{1 << 23, &sync.Pool{}}, // 8 MB
	{1 << 24, &sync.Pool{}}, // 16 MB
	{1 << 25, &sync.Pool{}}, // 32 MB
}

// EmptyBytesWithCapacity returns slice of length 0 with >= given capacity.
func EmptyBytesWithCapacity(capacity int) []byte {
	if p, ok := findPoolWithSize(capacity); ok {
		return getOrAllocate(p.pool, p.capacity)
	}

	// beyond largest bucket, allocate
	return make([]byte, 0, capacity)
}

// Clone clones given slice onto a slice from the cache.
func Clone(b []byte) []byte {
	return append(EmptyBytesWithCapacity(len(b)), b...)
}

// Return returns the given slice back to the pool.
func Return(b []byte) {
	if p, ok := findPoolWithSize(cap(b)); ok && p.capacity == cap(b) {
		p.pool.Put(&b)
	}
}

func findPoolWithSize(capacity int) (poolWithCapacity, bool) {
	// quick binary search to find the right pool bucket
	l, r := 0, len(pools)

	for l < r {
		if m := (l + r) >> 1; pools[m].capacity < capacity {
			l = m + 1
		} else {
			r = m
		}
	}

	if l < len(pools) {
		return pools[l], true
	}

	return poolWithCapacity{}, false
}

func getOrAllocate(p *sync.Pool, capacity int) []byte {
	v := p.Get()
	if v == nil {
		return make([]byte, 0, capacity)
	}

	return (*v.(*[]byte))[:0]
}
