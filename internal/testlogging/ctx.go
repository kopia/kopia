// Package testlogging implements logger that writes to testing.T log.
package testlogging

import (
	"context"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/repo/logging"
)

type testingT interface {
	Helper()
	Errorf(msg string, args ...any)
	Fatalf(msg string, args ...any)
	Logf(msg string, args ...any)
}

// Level specifies log level.
type Level = zapcore.Level

// log levels.
const (
	LevelDebug = zapcore.DebugLevel
	LevelInfo  = zapcore.InfoLevel
	LevelWarn  = zapcore.WarnLevel
	LevelError = zapcore.ErrorLevel
)

// NewTestLogger returns logger bound to the provided testing.T.
//
//nolint:thelper
func NewTestLogger(t *testing.T) logging.Logger {
	return Printf(t.Logf, "")
}

// Context returns a context with attached logger that emits all log entries to go testing.T log output.
func Context(t testingT) context.Context {
	return ContextWithLevel(t, LevelDebug)
}

// ContextWithLevel returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevel(t testingT, level Level) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return PrintfLevel(t.Logf, "["+module+"] ", level)
	})
}

// ContextWithLevelAndPrefix returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevelAndPrefix(t testingT, level Level, prefix string) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return PrintfLevel(t.Logf, "["+module+"] "+prefix, level)
	})
}

// ContextWithLevelAndPrefixFunc returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevelAndPrefixFunc(t testingT, level Level, prefixFunc func() string) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return PrintfLevel(t.Logf, "["+module+"] "+prefixFunc(), level)
	})
}
