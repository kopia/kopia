// Package logging provides loggers for Kopia.
package logging

import (
	"context"
)

// LoggerFactory retrieves logger for a given module.
type LoggerFactory func(module string) Logger

// Logger is an interface used by Kopia to output logs.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Debugw(msg string, keyValuePairs ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

// Module returns an function that returns a logger for a given module when provided with a context.
func Module(module string) func(ctx context.Context) Logger {
	return func(ctx context.Context) Logger {
		if l := ctx.Value(loggerCacheKey); l != nil {
			return l.(*loggerCache).getLogger(module) //nolint:forcetypeassert
		}

		return nullLogger{}
	}
}
