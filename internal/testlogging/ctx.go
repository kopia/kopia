// Package testlogging implements logger that writes to testing.T log.
package testlogging

import (
	"context"
	"testing"

	"github.com/kopia/kopia/repo/logging"
)

type testingT interface {
	Helper()
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	Logf(string, ...interface{})
}

// Level specifies log level.
type Level int

// log levels.
const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

type testLogger struct {
	t        testingT
	prefix   string
	minLevel Level
}

func (l *testLogger) Debugf(msg string, args ...interface{}) {
	if l.minLevel > LevelDebug {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+msg, args...)
}

func (l *testLogger) Debugw(msg string, keyValuePairs ...interface{}) {
	if l.minLevel > LevelDebug {
		return
	}

	l.t.Helper()
	l.t.Logf(logging.DebugMessageWithKeyValuePairs(msg, keyValuePairs))
}

func (l *testLogger) Infof(msg string, args ...interface{}) {
	if l.minLevel > LevelInfo {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+msg, args...)
}

func (l *testLogger) Warnf(msg string, args ...interface{}) {
	if l.minLevel > LevelWarn {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+msg, args...)
}

func (l *testLogger) Errorf(msg string, args ...interface{}) {
	if l.minLevel > LevelError {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+msg, args...)
}

var _ logging.Logger = &testLogger{}

// NewTestLogger returns logger bound to the provided testing.T.
// nolint:thelper
func NewTestLogger(t *testing.T) logging.Logger {
	return &testLogger{t, "", LevelDebug}
}

// Context returns a context with attached logger that emits all log entries to go testing.T log output.
func Context(t testingT) context.Context {
	return ContextWithLevel(t, LevelDebug)
}

// ContextWithLevel returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevel(t testingT, level Level) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return &testLogger{t, "[" + module + "] ", level}
	})
}

// ContextWithLevelAndPrefix returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevelAndPrefix(t testingT, level Level, prefix string) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return &testLogger{t, "[" + module + "] " + prefix, level}
	})
}

// ContextWithLevelAndPrefixFunc returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevelAndPrefixFunc(t testingT, level Level, prefixFunc func() string) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return &testLogger{t, "[" + module + "] " + prefixFunc(), level}
	})
}
