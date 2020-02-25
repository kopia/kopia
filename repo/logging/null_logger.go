package logging

type nullLogger struct{}

func (nullLogger) Debugf(msg string, args ...interface{})   {}
func (nullLogger) Infof(msg string, args ...interface{})    {}
func (nullLogger) Warningf(msg string, args ...interface{}) {}
func (nullLogger) Errorf(msg string, args ...interface{})   {}
func (nullLogger) Fatalf(msg string, args ...interface{})   {}

func getNullLogger(module string) Logger {
	return nullLogger{}
}
