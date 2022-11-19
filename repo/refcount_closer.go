package repo

import (
	"context"
	"sync/atomic"

	"go.uber.org/multierr"
)

// closeFunc is a function to invoke when the last repository reference is closed.
type closeFunc func(ctx context.Context) error

type refCountedCloser struct {
	refCount atomic.Int32
	closed   atomic.Bool

	closers []closeFunc
}

// Close decrements reference counter and invokes cleanup functions after last reference has been released.
func (c *refCountedCloser) Close(ctx context.Context) error {
	remaining := c.refCount.Add(-1)

	if remaining != 0 {
		return nil
	}

	if c.closed.Load() {
		panic("already closed")
	}

	c.closed.Store(true)

	var errors []error

	for _, closer := range c.closers {
		errors = append(errors, closer(ctx))
	}

	//nolint:wrapcheck
	return multierr.Combine(errors...)
}

func (c *refCountedCloser) addRef() {
	if c.closed.Load() {
		panic("already closed")
	}

	c.refCount.Add(1)
}

func newRefCountedCloser(f ...closeFunc) *refCountedCloser {
	rcc := &refCountedCloser{
		closers: f,
	}

	rcc.addRef()

	return rcc
}
