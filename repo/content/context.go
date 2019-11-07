package content

import "context"

type contextKey string

const (
	useContentCacheContextKey contextKey = "use-content-cache"
	useListCacheContextKey    contextKey = "use-list-cache"
)

// UsingContentCache returns a derived context that causes content manager to use cache.
func UsingContentCache(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, useContentCacheContextKey, enabled)
}

// UsingListCache returns a derived context that causes content manager to use cache.
func UsingListCache(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, useListCacheContextKey, enabled)
}

func shouldUseContentCache(ctx context.Context) bool {
	if enabled, ok := ctx.Value(useContentCacheContextKey).(bool); ok {
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
