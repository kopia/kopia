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

	return v.(Logger)
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
