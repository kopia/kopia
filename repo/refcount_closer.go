package repo

import (
	"context"
	stderrors "errors"
	"sync/atomic"
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

	// Set closed flag first before checking if already closed
	// This ensures addRef() racing with us will see the closed flag
	if !c.closed.CompareAndSwap(false, true) {
		// Already closed by another goroutine
		panic("already closed")
	}

	var errors []error

	for _, closer := range c.closers {
		errors = append(errors, closer(ctx))
	}

	return stderrors.Join(errors...)
}

func (c *refCountedCloser) addRef() {
	// Increment refcount first
	newCount := c.refCount.Add(1)

	// Then check if closed - this prevents the race where Close() sets closed=true
	// after we check but before we increment
	if c.closed.Load() {
		// We were already closed, decrement back and panic
		c.refCount.Add(-1)
		panic("already closed")
	}

	// Additional safety: if the count went negative, something is very wrong
	if newCount <= 0 {
		panic("refcount underflow")
	}
}

func (c *refCountedCloser) registerEarlyCloseFunc(f closeFunc) {
	c.closers = append(c.closers, append([]closeFunc(nil), f)...)
}

func newRefCountedCloser(f ...closeFunc) *refCountedCloser {
	rcc := &refCountedCloser{
		closers: f,
	}

	rcc.addRef()

	return rcc
}
