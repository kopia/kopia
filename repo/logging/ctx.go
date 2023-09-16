package logging

import (
	"context"
	"sync"
)

type contextKey string

const loggerCacheKey contextKey = "logger"

type loggerCache struct {
	createLoggerForModule LoggerFactory
	loggers               sync.Map
}

func (s *loggerCache) getLogger(module string) Logger {
	v, ok := s.loggers.Load(module)
	if !ok {
		v, _ = s.loggers.LoadOrStore(module, s.createLoggerForModule(module))
	}

	return v.(Logger) //nolint:forcetypeassert
}

// WithLogger returns a derived context with associated logger.
func WithLogger(ctx context.Context, l LoggerFactory) context.Context {
	if l == nil {
		l = getNullLogger
	}

	return context.WithValue(ctx, loggerCacheKey, &loggerCache{
		createLoggerForModule: l,
	})
}

// WithAdditionalLogger returns a context where all logging is emitted the original output plus the provided logger factory.
func WithAdditionalLogger(ctx context.Context, fact LoggerFactory) context.Context {
	originalLogFactory := loggerFactoryFromContext(ctx)

	return WithLogger(ctx, func(module string) Logger {
		return Broadcast(originalLogFactory(module), fact(module))
	})
}

// loggerFactoryFromContext returns a LoggerFactory associated with current context.
func loggerFactoryFromContext(ctx context.Context) LoggerFactory {
	v := ctx.Value(loggerCacheKey)
	if v == nil {
		return getNullLogger
	}

	return v.(*loggerCache).getLogger //nolint:forcetypeassert
}
