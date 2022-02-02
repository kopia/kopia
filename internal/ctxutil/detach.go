// Package ctxutil implements utilities for manipulating context.
package ctxutil

import (
	"context"
)

type detachedContext struct {
	// inherit most methods from context.Background()
	context.Context                 // nolint:containedctx
	wrapped         context.Context // nolint:containedctx
}

// Detach returns a context that inheris provided context's values but not deadline or cancellation.
func Detach(ctx context.Context) context.Context {
	return detachedContext{context.Background(), ctx}
}

func (d detachedContext) Value(key interface{}) interface{} {
	return d.wrapped.Value(key)
}
