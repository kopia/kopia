package storage

import "context"

type contextKey string

var progressCallbackContextKey contextKey = "progress-callback"

// ProgressFunc is used to report progress of a long-running storage operation.
type ProgressFunc func(desc string, completed, total int64)

// WithUploadProgressCallback returns a context that passes callback function to be used storage upload progress.
func WithUploadProgressCallback(ctx context.Context, callback ProgressFunc) context.Context {
	return context.WithValue(ctx, progressCallbackContextKey, callback)
}

// ProgressCallback gets the progress callback function from the context.
func ProgressCallback(ctx context.Context) ProgressFunc {
	pf, _ := ctx.Value(progressCallbackContextKey).(ProgressFunc)
	return pf
}
