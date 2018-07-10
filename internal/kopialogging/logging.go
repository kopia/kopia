// Package kopialogging provides loggers for the rest of codebase.
package kopialogging

import "github.com/op/go-logging"

// Logger returns an instance of a logger used throughout Kopia codebase.
func Logger(module string) *logging.Logger {
	return logging.MustGetLogger(module)
}
