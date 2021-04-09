// Package logging provides loggers for Kopia.
package logging

import (
	"context"
)

// defaultLoggerForModuleFunc is a logger to use when context-specific logger is not set.
var defaultLoggerForModuleFunc = getNullLogger

// LoggerForModuleFunc retrieves logger for a given module.
type LoggerForModuleFunc func(module string) Logger

// SetDefault sets the logger to use when context-specific logger is not set.
func SetDefault(l LoggerForModuleFunc) {
	if l == nil {
		defaultLoggerForModuleFunc = getNullLogger
	} else {
		defaultLoggerForModuleFunc = l
	}
}

// Logger is an interface used by Kopia to output logs.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

// GetContextLoggerFunc returns an function that returns a logger for a given module when provided with a context.
func GetContextLoggerFunc(module string) func(ctx context.Context) Logger {
	return func(ctx context.Context) Logger {
		if l := ctx.Value(loggerKey); l != nil {
			return l.(LoggerForModuleFunc)(module)
		}

		return defaultLoggerForModuleFunc(module)
	}
}
