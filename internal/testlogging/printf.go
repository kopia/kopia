package testlogging

import (
	"bytes"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/repo/logging"
)

// Printf returns a logger that uses given printf-style function to print log output.
func Printf(printf func(msg string, args ...interface{}), prefix string) *zap.SugaredLogger {
	return PrintfLevel(printf, prefix, zapcore.DebugLevel)
}

// PrintfLevel returns a logger that uses given printf-style function to print log output for logs of a given level or above.
func PrintfLevel(printf func(msg string, args ...interface{}), prefix string, level zapcore.Level) *zap.SugaredLogger {
	writer := printfWriter{printf, prefix}

	return zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
				// Keys can be anything except the empty string.
				TimeKey:        zapcore.OmitKey,
				LevelKey:       zapcore.OmitKey,
				NameKey:        zapcore.OmitKey,
				CallerKey:      zapcore.OmitKey,
				FunctionKey:    zapcore.OmitKey,
				MessageKey:     "M",
				StacktraceKey:  "S",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}),
			writer,
			level,
		),
	).Sugar()
}

// PrintfFactory returns LoggerForModuleFunc that uses given printf-style function to print log output.
func PrintfFactory(printf func(msg string, args ...interface{})) logging.LoggerFactory {
	return func(module string) *zap.SugaredLogger {
		return Printf(printf, "["+module+"] ")
	}
}

type printfWriter struct {
	printf func(msg string, args ...interface{})
	prefix string
}

func (w printfWriter) Write(p []byte) (int, error) {
	n := len(p)

	w.printf("%s%s", w.prefix, bytes.TrimRight(p, "\n"))

	return n, nil
}

func (w printfWriter) Sync() error {
	return nil
}
