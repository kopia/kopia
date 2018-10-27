// Package repologging provides loggers.
package repologging

import "github.com/op/go-logging"

// Logger returns an instance of a logger used throughout repository codebase.
func Logger(module string) *logging.Logger {
	return logging.MustGetLogger(module)
}
