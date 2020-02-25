// Package testlogging implements logger that writes to testing.T log.
package testlogging

import (
	"context"
	"testing"

	"github.com/kopia/kopia/repo/logging"
)

// Level specifies log level.
type Level int

// log levels
const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarning
	LevelError
	LevelFatal
)

type testLogger struct {
	t        *testing.T
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
func (l *testLogger) Infof(msg string, args ...interface{}) {
	if l.minLevel > LevelInfo {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+msg, args...)
}
func (l *testLogger) Warningf(msg string, args ...interface{}) {
	if l.minLevel > LevelWarning {
		return
	}

	l.t.Helper()
	l.t.Logf(l.prefix+"warning: "+msg, args...)
}
func (l *testLogger) Errorf(msg string, args ...interface{}) {
	if l.minLevel > LevelError {
		return
	}

	l.t.Helper()
	l.t.Errorf(l.prefix+msg, args...)
}
func (l *testLogger) Fatalf(msg string, args ...interface{}) {
	if l.minLevel > LevelFatal {
		return
	}

	l.t.Helper()
	l.t.Fatalf(l.prefix+msg, args...)
}

var _ logging.Logger = &testLogger{}

// Context returns a context with attached logger that emits all log entries to go testing.T log output.
func Context(t *testing.T) context.Context {
	return ContextWithLevel(t, LevelDebug)
}

// ContextWithLevel returns a context with attached logger that emits all log entries with given log level or above.
func ContextWithLevel(t *testing.T, level Level) context.Context {
	return logging.WithLogger(context.Background(), func(module string) logging.Logger {
		return &testLogger{t, "[" + module + "] ", level}
	})
}
