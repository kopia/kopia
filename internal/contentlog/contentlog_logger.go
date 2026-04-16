package contentlog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"strings"
	"time"

	"github.com/kopia/kopia/internal/clock"
)

// WriterTo is a type that can write itself to a JSON writer.
type WriterTo interface {
	WriteTo(jw *JSONWriter)
}

type loggerParamsKeyType string

const loggerParamsKey loggerParamsKeyType = "loggerParams"

// Emit writes the entry to the segment writer.
// We are using this particular syntax to avoid allocating an intermediate interface value.
// This allows exactly zero non-amortized allocations in all cases.
func Emit[T WriterTo](ctx context.Context, l *Logger, entry T) {
	if l == nil {
		return
	}

	if l.output == nil {
		return
	}

	jw := NewJSONWriter()
	defer jw.Release()

	jw.BeginObject()
	jw.TimeField("t", l.timeFunc())

	for _, param := range l.params {
		param.WriteValueTo(jw)
	}

	params := ctx.Value(loggerParamsKey)
	if params != nil {
		if params, ok := params.([]ParamWriter); ok {
			for _, p := range params {
				p.WriteValueTo(jw)
			}
		}
	}

	entry.WriteTo(jw)
	jw.EndObject()
	jw.buf = append(jw.buf, '\n')

	l.output(jw.buf)
}

// Log logs a message with no parameters.
func Log(ctx context.Context, l *Logger, text string) {
	Emit(ctx, l, debugMessageWithParams[voidParamValue, voidParamValue, voidParamValue, voidParamValue, voidParamValue, voidParamValue]{text: text})
}

// Log1 logs a message with one parameter.
func Log1[T1 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1) {
	Emit(ctx, l, debugMessageWithParams[T1, voidParamValue, voidParamValue, voidParamValue, voidParamValue, voidParamValue]{text: format, v1: value1})
}

// Log2 logs a message with two parameters.
func Log2[T1, T2 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1, value2 T2) {
	Emit(ctx, l, debugMessageWithParams[T1, T2, voidParamValue, voidParamValue, voidParamValue, voidParamValue]{text: format, v1: value1, v2: value2})
}

// Log3 logs a message with three parameters.
func Log3[T1, T2, T3 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1, value2 T2, value3 T3) {
	Emit(ctx, l, debugMessageWithParams[T1, T2, T3, voidParamValue, voidParamValue, voidParamValue]{text: format, v1: value1, v2: value2, v3: value3})
}

// Log4 logs a message with four parameters.
func Log4[T1, T2, T3, T4 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1, value2 T2, value3 T3, value4 T4) {
	Emit(ctx, l, debugMessageWithParams[T1, T2, T3, T4, voidParamValue, voidParamValue]{text: format, v1: value1, v2: value2, v3: value3, v4: value4})
}

// Log5 logs a message with five parameters.
func Log5[T1, T2, T3, T4, T5 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1, value2 T2, value3 T3, value4 T4, value5 T5) {
	Emit(ctx, l, debugMessageWithParams[T1, T2, T3, T4, T5, voidParamValue]{text: format, v1: value1, v2: value2, v3: value3, v4: value4, v5: value5})
}

// Log6 logs a message with six parameters.
func Log6[T1, T2, T3, T4, T5, T6 ParamWriter](ctx context.Context, l *Logger, format string, value1 T1, value2 T2, value3 T3, value4 T4, value5 T5, value6 T6) {
	Emit(ctx, l, debugMessageWithParams[T1, T2, T3, T4, T5, T6]{text: format, v1: value1, v2: value2, v3: value3, v4: value4, v5: value5, v6: value6})
}

// WithParams returns a new logger with the given parameters.
func WithParams(ctx context.Context, params ...ParamWriter) context.Context {
	existing := ctx.Value(loggerParamsKey)
	if existing != nil {
		if existing, ok := existing.([]ParamWriter); ok {
			params = append(append([]ParamWriter(nil), existing...), params...)
		}
	}

	return context.WithValue(ctx, loggerParamsKey, params)
}

type voidParamValue struct{}

func (e voidParamValue) WriteValueTo(*JSONWriter) {}

type debugMessageWithParams[T1 ParamWriter, T2 ParamWriter, T3 ParamWriter, T4 ParamWriter, T5 ParamWriter, T6 ParamWriter] struct {
	text string
	v1   T1
	v2   T2
	v3   T3
	v4   T4
	v5   T5
	v6   T6
}

func (e debugMessageWithParams[T1, T2, T3, T4, T5, T6]) WriteTo(jw *JSONWriter) {
	jw.StringField("m", e.text)
	e.v1.WriteValueTo(jw)
	e.v2.WriteValueTo(jw)
	e.v3.WriteValueTo(jw)
	e.v4.WriteValueTo(jw)
	e.v5.WriteValueTo(jw)
	e.v6.WriteValueTo(jw)
}

// Logger is a logger that writes log entries to the output.
type Logger struct {
	params   []ParamWriter // Parameters to include in each log entry.
	output   OutputFunc
	timeFunc func() time.Time
}

// OutputFunc is a function that writes the log entry to the output.
type OutputFunc func(data []byte)

// NewLogger creates a new logger.
func NewLogger(out OutputFunc, params ...ParamWriter) *Logger {
	return &Logger{
		params:   params,
		output:   out,
		timeFunc: clock.Now,
	}
}

// RandomSpanID generates a random span ID (40 bits encoded as 5 base32 characters == 8 ASCII characters).
func RandomSpanID() string {
	var runID [5]byte

	rand.Read(runID[:]) //nolint:errcheck

	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(runID[:]))
}

// HashSpanID hashes a given value a Span ID.
func HashSpanID(v string) string {
	spanID := sha256.Sum256([]byte(v))
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(spanID[:10]))
}
