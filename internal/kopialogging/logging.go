package kopialogging

import "github.com/op/go-logging"

func Logger(module string) *logging.Logger {
	return logging.MustGetLogger(module)
}
