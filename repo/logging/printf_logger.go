package logging

type printfLogger struct {
	printf func(msg string, args ...interface{})
	prefix string
}

func (l *printfLogger) Debugf(msg string, args ...interface{}) { l.printf(l.prefix+msg, args...) }
func (l *printfLogger) Infof(msg string, args ...interface{})  { l.printf(l.prefix+msg, args...) }
func (l *printfLogger) Errorf(msg string, args ...interface{}) { l.printf(l.prefix+msg, args...) }

// Printf returns LoggerForModuleFunc that uses given printf-style function to print log output.
func Printf(printf func(msg string, args ...interface{})) LoggerForModuleFunc {
	return func(module string) Logger {
		return &printfLogger{printf, "[" + module + "]"}
	}
}
