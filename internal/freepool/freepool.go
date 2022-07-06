// Package freepool manages a free pool of objects that are expensive to create.
package freepool

import (
	"sync"
)

// Pool is a small pool of recently returned objects.
// Unlike sync.Pool, the pool is not subject to gargbage collection under memory pressure.
type Pool struct {
	clean func(v interface{})
	pool  sync.Pool
}

// Take returns an item from the pool, and if not available makes a new one.
func (p *Pool) Take() interface{} {
	return p.pool.Get()
}

// Return returns an item to the pool after cleaning it.
func (p *Pool) Return(v interface{}) {
	p.clean(v)
	p.pool.Put(v)
}

// New returns a new free pool.
func New(makeNew func() interface{}, clean func(v interface{})) *Pool {
	return &Pool{
		clean: clean,
		pool: sync.Pool{
			New: makeNew,
		},
	}
}
