package logging

type nullLogger struct{}

func (nullLogger) Debugf(msg string, args ...interface{}) {}
func (nullLogger) Infof(msg string, args ...interface{})  {}
func (nullLogger) Errorf(msg string, args ...interface{}) {}

var nullLoggerInstance = nullLogger{}

func getNullLogger(module string) Logger {
	return nullLoggerInstance
}
