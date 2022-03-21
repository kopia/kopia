package object

import "context"

type readerOptionsKey struct{}

// ReaderOptions encapsulates options used when reading repository objects.
type ReaderOptions struct {
	ReadAheadBytes int64 `json:"readAheadBytes"`
}

// WithReaderOptions attaches the provided reader options to the context.
// All repository reads using that context will be subject to the options.
func WithReaderOptions(ctx context.Context, opts ReaderOptions) context.Context {
	return context.WithValue(ctx, readerOptionsKey{}, &opts)
}

// ReaderOptionsFromContext returns ReaderOptions for the given context.
func ReaderOptionsFromContext(ctx context.Context) ReaderOptions {
	v := ctx.Value(readerOptionsKey{})
	if v == nil {
		// use heuristics
		return ReaderOptions{}
	}

	// nolint:forcetypeassert
	return *v.(*ReaderOptions)
}
