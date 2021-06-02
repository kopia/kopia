package logging

// Broadcast is a logger that broadcasts each log message to multiple loggers.
type Broadcast []Logger

// Debugf implements Logger.
func (b Broadcast) Debugf(msg string, args ...interface{}) {
	for _, l := range b {
		l.Debugf(msg, args...)
	}
}

// Infof implements Logger.
func (b Broadcast) Infof(msg string, args ...interface{}) {
	for _, l := range b {
		l.Infof(msg, args...)
	}
}

// Errorf implements Logger.
func (b Broadcast) Errorf(msg string, args ...interface{}) {
	for _, l := range b {
		l.Errorf(msg, args...)
	}
}

var _ Logger = Broadcast{}
