// Package freepool manages a free pool of objects that are expensive to create.
package freepool

import (
	"sync"
)

// Pool is a small pool of recently returned objects with built-in cleanup.
type Pool[T any] struct {
	clean func(v *T)
	pool  sync.Pool
}

// Take returns an item from the pool, and if not available makes a new one.
func (p *Pool[T]) Take() *T {
	//nolint:forcetypeassert
	return p.pool.Get().(*T)
}

// Return returns an item to the pool after cleaning it.
func (p *Pool[T]) Return(v *T) {
	p.clean(v)
	p.pool.Put(v)
}

// New returns a new free pool.
func New[T any](makeNew func() *T, clean func(v *T)) *Pool[T] {
	return &Pool[T]{
		clean: clean,
		pool: sync.Pool{
			New: func() any {
				return makeNew()
			},
		},
	}
}

// NewStruct returns a pool that produces provided clean structures.
func NewStruct[T any](cleanItem T) *Pool[T] {
	return New(func() *T {
		r := cleanItem
		return &r
	}, func(v *T) {
		*v = cleanItem
	})
}
