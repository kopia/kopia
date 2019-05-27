package block

import "context"

type contextKey string

var useBlockCacheContextKey contextKey = "use-block-cache"
var useListCacheContextKey contextKey = "use-list-cache"

// UsingBlockCache returns a derived context that causes block manager to use cache.
func UsingBlockCache(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, useBlockCacheContextKey, enabled)
}

// UsingListCache returns a derived context that causes block manager to use cache.
func UsingListCache(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, useListCacheContextKey, enabled)
}

func shouldUseBlockCache(ctx context.Context) bool {
	if enabled, ok := ctx.Value(useBlockCacheContextKey).(bool); ok {
		return enabled
	}

	return true
}

func shouldUseListCache(ctx context.Context) bool {
	if enabled, ok := ctx.Value(useListCacheContextKey).(bool); ok {
		return enabled
	}

	return true
}
