package testlogging

import (
	"bytes"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/internal/zaplogutil"
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
			zaplogutil.NewStdConsoleEncoder(zaplogutil.StdConsoleEncoderConfig{}),
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
