package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Broadcast is a logger that broadcasts each log message to multiple loggers.
func Broadcast(logger ...Logger) Logger {
	var cores []zapcore.Core

	var singleName string

	for _, l := range logger {
		dl := l.Desugar()

		if singleName == "" {
			singleName = dl.Name()
		}

		if dl.Name() != singleName {
			singleName = "-"
		}

		cores = append(cores, dl.Core())
	}

	return zap.New(zapcore.NewTee(cores...)).Sugar().Named(singleName)
}
