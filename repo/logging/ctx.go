package logging

import "context"

type contextKey string

const loggerKey contextKey = "logger"

// WithLogger returns a derived context with associated logger.
func WithLogger(ctx context.Context, l LoggerForModuleFunc) context.Context {
	if l == nil {
		l = getNullLogger
	}

	return context.WithValue(ctx, loggerKey, l)
}
