package logging

type nullLogger struct{}

func (nullLogger) Debugf(msg string, args ...interface{})          {}
func (nullLogger) Debugw(msg string, keyValuePairs ...interface{}) {}
func (nullLogger) Infof(msg string, args ...interface{})           {}
func (nullLogger) Warnf(msg string, args ...interface{})           {}
func (nullLogger) Errorf(msg string, args ...interface{})          {}

// NullLogger is a null logger that discards all log messages.
func NullLogger() Logger {
	return nullLogger{}
}

func getNullLogger(module string) Logger {
	return nullLogger{}
}
