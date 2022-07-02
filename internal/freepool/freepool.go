// Package freepool manages a free pool of objects that are expensive to create.
package freepool

import (
	"github.com/golang-design/lockfree"
)

// Pool is a small pool of recently returned objects.
// Unlike sync.Pool, the pool is not subject to gargbage collection under memory pressure.
type Pool struct {
	makeNew func() interface{}
	clean   func(v interface{})
	stack   lockfree.Stack
}

// Take returns an item from the pool, and if not available makes a new one.
func (p *Pool) Take() interface{} {
	v := p.stack.Pop()
	if v == nil {
		return p.makeNew()
	}

	return v
}

// Return returns an item to the pool after cleaning it.
func (p *Pool) Return(v interface{}) {
	p.clean(v)

	p.stack.Push(v)
}

// New returns a new free pool.
func New(makeNew func() interface{}, clean func(v interface{})) *Pool {
	return &Pool{
		makeNew: makeNew,
		clean:   clean,
	}
}
