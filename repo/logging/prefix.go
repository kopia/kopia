package logging

// prefixLogger is a logger that attaches a prefix to each log message.
type prefixLogger struct {
	prefixFunc func() string
	inner      Logger
}

// Debugf implements Logger.
func (p *prefixLogger) Debugf(msg string, args ...interface{}) {
	p.inner.Debugf(p.prefixFunc()+msg, args...)
}

// Infof implements Logger.
func (p *prefixLogger) Infof(msg string, args ...interface{}) {
	p.inner.Infof(p.prefixFunc()+msg, args...)
}

// Errorf implements Logger.
func (p *prefixLogger) Errorf(msg string, args ...interface{}) {
	p.inner.Errorf(p.prefixFunc()+msg, args...)
}

var _ Logger = (*prefixLogger)(nil)

// WithPrefix returns a wrapper logger that attaches given prefix to each message.
func WithPrefix(prefix string, logger Logger) Logger {
	return &prefixLogger{func() string { return prefix }, logger}
}
