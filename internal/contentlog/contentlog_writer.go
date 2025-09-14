package contentlog

// Logger is a logger that writes log entries to the output.
type Logger struct {
	params []ParamWriter // Parameters to include in each log entry.
	output OutputFunc
}

// OutputFunc is a function that writes the log entry to the output.
type OutputFunc func(data []byte)

// NewLogger creates a new logger.
func NewLogger(out OutputFunc, params ...ParamWriter) *Logger {
	return &Logger{
		params: params,
		output: out,
	}
}
