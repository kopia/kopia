package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Broadcast is a logger that broadcasts each log message to multiple loggers.
func Broadcast(logger ...Logger) Logger {
	var cores []zapcore.Core

	for _, l := range logger {
		cores = append(cores, l.Desugar().Core())
	}

	return zap.New(zapcore.NewTee(cores...)).Sugar()
}
