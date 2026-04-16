// Package contentlog provides a JSON writer that can write JSON to a buffer
// without any memory allocations and Logger that can write log entries
// with strongly-typed parameters.
package contentlog

import (
	"strconv"
	"time"

	"github.com/kopia/kopia/internal/freepool"
)

var commaSeparator = []byte(",")

const (
	decimal     = 10
	hexadecimal = 16
)

// JSONWriter is a writer that can write JSON to a buffer
// without any memory allocations.
type JSONWriter struct {
	buf       []byte
	separator []byte

	separatorStack [][]byte
}

// ParamWriter must be implemented by all types that write a parameter ("key":value)to the JSON writer.
type ParamWriter interface {
	WriteValueTo(jw *JSONWriter)
}

func (jw *JSONWriter) beforeField(key string) {
	jw.buf = append(jw.buf, jw.separator...)
	jw.buf = append(jw.buf, '"')
	jw.buf = append(jw.buf, key...)
	jw.buf = append(jw.buf, '"', ':')
	jw.separator = commaSeparator
}

// RawJSONField writes a raw JSON field where the value is already in JSON format.
func (jw *JSONWriter) RawJSONField(key string, value []byte) {
	jw.beforeField(key)
	jw.buf = append(jw.buf, value...)
}

func (jw *JSONWriter) beforeElement() {
	jw.buf = append(jw.buf, jw.separator...)
	jw.separator = commaSeparator
}

// object

// BeginObjectField starts an object field.
func (jw *JSONWriter) BeginObjectField(key string) {
	jw.beforeField(key)
	jw.BeginObject()
}

// BeginObject starts an object.
func (jw *JSONWriter) BeginObject() {
	jw.buf = append(jw.buf, '{')
	jw.separatorStack = append(jw.separatorStack, commaSeparator)
	jw.separator = nil
}

// EndObject ends an object.
func (jw *JSONWriter) EndObject() {
	jw.separator = jw.separatorStack[len(jw.separatorStack)-1]
	jw.separatorStack = jw.separatorStack[:len(jw.separatorStack)-1]
	jw.buf = append(jw.buf, '}')
}

// list

// BeginListField starts a list field.
func (jw *JSONWriter) BeginListField(key string) {
	jw.beforeField(key)
	jw.BeginList()
}

// BeginList starts a list.
func (jw *JSONWriter) BeginList() {
	jw.buf = append(jw.buf, '[')
	jw.separatorStack = append(jw.separatorStack, commaSeparator)
	jw.separator = nil
}

// EndList ends a list.
func (jw *JSONWriter) EndList() {
	jw.separator = jw.separatorStack[len(jw.separatorStack)-1]
	jw.separatorStack = jw.separatorStack[:len(jw.separatorStack)-1]
	jw.buf = append(jw.buf, ']')
}

// string

// StringField writes a string field.
func (jw *JSONWriter) StringField(key, value string) {
	jw.beforeField(key)
	jw.stringValue(value)
}

// StringElement writes a string element.
func (jw *JSONWriter) StringElement(value string) {
	jw.beforeElement()
	jw.stringValue(value)
}

func (jw *JSONWriter) stringValue(value string) {
	jw.buf = append(jw.buf, '"')

	for i := range len(value) {
		c := value[i]

		//nolint:gocritic
		if c < ' ' {
			switch c {
			case '\b':
				jw.buf = append(jw.buf, '\\', 'b')
			case '\f':
				jw.buf = append(jw.buf, '\\', 'f')
			case '\n':
				jw.buf = append(jw.buf, '\\', 'n')
			case '\r':
				jw.buf = append(jw.buf, '\\', 'r')
			case '\t':
				jw.buf = append(jw.buf, '\\', 't')
			default:
				// Escape as unicode \u00XX
				jw.buf = append(jw.buf, '\\', 'u', '0', '0')

				var hexBuf [8]byte

				hex := strconv.AppendInt(hexBuf[:0], int64(c), hexadecimal)
				if len(hex) < 2 { //nolint:mnd
					jw.buf = append(jw.buf, '0')
				}

				jw.buf = append(jw.buf, hex...)
			}
		} else if c == '"' {
			jw.buf = append(jw.buf, '\\', '"')
		} else if c == '\\' {
			jw.buf = append(jw.buf, '\\', '\\')
		} else {
			jw.buf = append(jw.buf, c)
		}
	}

	jw.buf = append(jw.buf, '"')
}

// null

// NullElement writes a null element.
func (jw *JSONWriter) NullElement() {
	jw.beforeElement()
	jw.nullValue()
}

// NullField writes a null field.
func (jw *JSONWriter) NullField(key string) {
	jw.beforeField(key)
	jw.nullValue()
}

func (jw *JSONWriter) nullValue() {
	jw.buf = append(jw.buf, "null"...)
}

// boolean

// BoolField writes a boolean field.
func (jw *JSONWriter) BoolField(key string, value bool) {
	jw.beforeField(key)
	jw.boolValue(value)
}

// BoolElement writes a boolean element.
func (jw *JSONWriter) BoolElement(value bool) {
	jw.beforeElement()
	jw.boolValue(value)
}

func (jw *JSONWriter) boolValue(value bool) {
	if value {
		jw.buf = append(jw.buf, "true"...)
	} else {
		jw.buf = append(jw.buf, "false"...)
	}
}

// signed integers

// IntField writes an int field.
func (jw *JSONWriter) IntField(key string, value int) { jw.Int64Field(key, int64(value)) }

// IntElement writes an int element.
func (jw *JSONWriter) IntElement(value int) { jw.Int64Element(int64(value)) }

// Int8Field writes an int8 field.
func (jw *JSONWriter) Int8Field(key string, value int8) { jw.Int64Field(key, int64(value)) }

// Int8Element writes an int8 element.
func (jw *JSONWriter) Int8Element(value int8) { jw.Int64Element(int64(value)) }

// Int16Field writes an int16 field.
func (jw *JSONWriter) Int16Field(key string, value int16) { jw.Int64Field(key, int64(value)) }

// Int16Element writes an int16 element.
func (jw *JSONWriter) Int16Element(value int16) { jw.Int64Element(int64(value)) }

// Int32Field writes an int32 field.
func (jw *JSONWriter) Int32Field(key string, value int32) { jw.Int64Field(key, int64(value)) }

// Int32Element writes an int32 element.
func (jw *JSONWriter) Int32Element(value int32) { jw.Int64Element(int64(value)) }

// Int64Field writes an int64 field.
func (jw *JSONWriter) Int64Field(key string, value int64) {
	jw.beforeField(key)
	jw.int64Value(value)
}

// Int64Element writes an int64 element.
func (jw *JSONWriter) Int64Element(value int64) {
	jw.beforeElement()
	jw.int64Value(value)
}

func (jw *JSONWriter) int64Value(value int64) {
	var buf [64]byte

	jw.buf = append(jw.buf, strconv.AppendInt(buf[:0], value, decimal)...)
}

// unsigned integers

// UIntField writes a uint field.
func (jw *JSONWriter) UIntField(key string, value uint) { jw.UInt64Field(key, uint64(value)) }

// UIntElement writes a uint element.
func (jw *JSONWriter) UIntElement(value uint) { jw.UInt64Element(uint64(value)) }

// UInt8Field writes a uint8 field.
func (jw *JSONWriter) UInt8Field(key string, value uint8) { jw.UInt64Field(key, uint64(value)) }

// UInt8Element writes a uint8 element.
func (jw *JSONWriter) UInt8Element(value uint8) { jw.UInt64Element(uint64(value)) }

// UInt16Field writes a uint16 field.
func (jw *JSONWriter) UInt16Field(key string, value uint16) { jw.UInt64Field(key, uint64(value)) }

// UInt16Element writes a uint16 element.
func (jw *JSONWriter) UInt16Element(value uint16) { jw.UInt64Element(uint64(value)) }

// UInt32Field writes a uint32 field.
func (jw *JSONWriter) UInt32Field(key string, value uint32) { jw.UInt64Field(key, uint64(value)) }

// UInt32Element writes a uint32 element.
func (jw *JSONWriter) UInt32Element(value uint32) { jw.UInt64Element(uint64(value)) }

// UInt64Field writes a uint64 field.
func (jw *JSONWriter) UInt64Field(key string, value uint64) {
	jw.beforeField(key)
	jw.uint64Value(value)
}

// UInt64Element writes a uint64 element.
func (jw *JSONWriter) UInt64Element(value uint64) {
	jw.beforeElement()
	jw.uint64Value(value)
}

func (jw *JSONWriter) uint64Value(value uint64) {
	var buf [64]byte

	jw.buf = append(jw.buf, strconv.AppendUint(buf[:0], value, decimal)...)
}

// error

// ErrorField writes an error field.
func (jw *JSONWriter) ErrorField(key string, value error) {
	if value == nil {
		jw.NullField(key)
	} else {
		jw.StringField(key, value.Error())
	}
}

// time

// TimeField writes a time field.
func (jw *JSONWriter) TimeField(key string, value time.Time) {
	jw.beforeField(key)
	jw.timeValue(value)
}

// appendPaddedInt appends an integer with zero-padding to the buffer.
func (jw *JSONWriter) appendPaddedInt(value int64, width int) {
	var numBuf [64]byte

	numStr := strconv.AppendInt(numBuf[:0], value, decimal)
	numLen := len(numStr)

	// Add leading zeros
	for i := numLen; i < width; i++ {
		jw.buf = append(jw.buf, '0')
	}

	jw.buf = append(jw.buf, numStr...)
}

// TimeElement writes a time element.
func (jw *JSONWriter) TimeElement(value time.Time) {
	jw.beforeElement()
	jw.timeValue(value)
}

func (jw *JSONWriter) timeValue(value time.Time) {
	utc := value.UTC()

	jw.buf = append(jw.buf, '"')
	jw.appendPaddedInt(int64(utc.Year()), 4) //nolint:mnd
	jw.buf = append(jw.buf, '-')
	jw.appendPaddedInt(int64(utc.Month()), 2) //nolint:mnd
	jw.buf = append(jw.buf, '-')
	jw.appendPaddedInt(int64(utc.Day()), 2) //nolint:mnd
	jw.buf = append(jw.buf, 'T')
	jw.appendPaddedInt(int64(utc.Hour()), 2) //nolint:mnd
	jw.buf = append(jw.buf, ':')
	jw.appendPaddedInt(int64(utc.Minute()), 2) //nolint:mnd
	jw.buf = append(jw.buf, ':')
	jw.appendPaddedInt(int64(utc.Second()), 2) //nolint:mnd
	jw.buf = append(jw.buf, '.')
	jw.appendPaddedInt(int64(utc.Nanosecond()/1000), 6) //nolint:mnd
	jw.buf = append(jw.buf, 'Z', '"')
}

var freeJSONWriterPool = freepool.New(
	func() *JSONWriter {
		return &JSONWriter{
			buf:            make([]byte, 0, 1024), //nolint:mnd
			separatorStack: make([][]byte, 0, 10), //nolint:mnd
			separator:      nil,
		}
	}, func(jw *JSONWriter) {
		jw.buf = jw.buf[:0]
		jw.separatorStack = jw.separatorStack[:0]
		jw.separator = nil
	})

// Release releases the JSON writer back to the pool.
func (jw *JSONWriter) Release() {
	freeJSONWriterPool.Return(jw)
}

// Result returns the internal buffer for testing purposes.
// This should only be used in tests.
func (jw *JSONWriter) Result() []byte {
	return jw.buf
}

// NewJSONWriter creates a new JSON writer.
func NewJSONWriter() *JSONWriter {
	return freeJSONWriterPool.Take()
}

// GetBufferForTesting returns the internal buffer for testing purposes.
// This should only be used in tests.
func (jw *JSONWriter) GetBufferForTesting() []byte {
	return jw.buf
}
