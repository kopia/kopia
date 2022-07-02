// Package logging provides loggers for Kopia.
package logging

import (
	"context"
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/internal/zaplogutil"
)

// Logger is used by Kopia to emit various logs.
type Logger = *zap.SugaredLogger

// LoggerFactory retrieves a named logger for a given module.
type LoggerFactory func(module string) Logger

// Module returns an function that returns a logger for a given module when provided with a context.
func Module(module string) func(ctx context.Context) Logger {
	return func(ctx context.Context) Logger {
		if l := ctx.Value(loggerCacheKey); l != nil {
			return l.(*loggerCache).getLogger(module) //nolint:forcetypeassert
		}

		return NullLogger
	}
}

// ToWriter returns LoggerFactory that uses given writer for log output (unadorned).
func ToWriter(w io.Writer) LoggerFactory {
	return zap.New(zapcore.NewCore(
		zaplogutil.NewStdConsoleEncoder(zaplogutil.StdConsoleEncoderConfig{}),
		zapcore.AddSync(w), zap.DebugLevel), zap.WithClock(zaplogutil.Clock())).Sugar().Named
}
