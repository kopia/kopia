package logging

import (
	"fmt"
	"io"
	"strings"
)

// DebugMessageWithKeyValuePairs returns a debug string consisting of alternating key:value separated by spaces and prefixed with a space.
func DebugMessageWithKeyValuePairs(msg string, keyValuePairs []interface{}) string {
	var sb strings.Builder

	sb.WriteString(msg)

	for i := 0; i+1 < len(keyValuePairs); i += 2 {
		if key, ok := keyValuePairs[i].(string); ok {
			value := keyValuePairs[i+1]

			if stringValue, ok := keyValuePairs[i+1].(string); ok {
				fmt.Fprintf(&sb, " %v:%q", key, stringValue)
			} else {
				fmt.Fprintf(&sb, " %v:%v", key, value)
			}
		} else {
			fmt.Fprintf(&sb, " malformed-%v:%v", keyValuePairs[i], keyValuePairs[i+1])
		}
	}

	if len(keyValuePairs)%2 == 1 {
		fmt.Fprintf(&sb, " malformed-%v", keyValuePairs[len(keyValuePairs)-1])
	}

	return sb.String()
}

type printfLogger struct {
	printf func(msg string, args ...interface{})
	prefix string
}

func (l *printfLogger) Debugf(msg string, args ...interface{}) { l.printf(l.prefix+msg, args...) }

func (l *printfLogger) Debugw(msg string, keyValuePairs ...interface{}) {
	l.printf(DebugMessageWithKeyValuePairs(l.prefix+msg, keyValuePairs))
}

func (l *printfLogger) Infof(msg string, args ...interface{})  { l.printf(l.prefix+msg, args...) }
func (l *printfLogger) Warnf(msg string, args ...interface{})  { l.printf(l.prefix+msg, args...) }
func (l *printfLogger) Errorf(msg string, args ...interface{}) { l.printf(l.prefix+msg, args...) }

// Printf returns a logger that uses given printf-style function to print log output.
func Printf(printf func(msg string, args ...interface{}), prefix string) Logger {
	return &printfLogger{printf, prefix}
}

// PrintfFactory returns LoggerForModuleFunc that uses given printf-style function to print log output.
func PrintfFactory(printf func(msg string, args ...interface{})) LoggerFactory {
	return func(module string) Logger {
		return &printfLogger{printf, "[" + module + "] "}
	}
}

// Writer returns LoggerForModuleFunc that uses given writer for log output.
func Writer(w io.Writer) LoggerFactory {
	printf := func(msg string, args ...interface{}) {
		msg = fmt.Sprintf(msg, args...)

		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}

		io.WriteString(w, msg) //nolint:errcheck
	}

	return func(module string) Logger {
		return &printfLogger{printf, ""}
	}
}
