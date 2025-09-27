package logparam

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/contentlog"
)

func TestString(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		expected map[string]any
	}{
		{
			name:  "simple string",
			key:   "message",
			value: "hello world",
			expected: map[string]any{
				"message": "hello world",
			},
		},
		{
			name:  "empty string",
			key:   "empty",
			value: "",
			expected: map[string]any{
				"empty": "",
			},
		},
		{
			name:  "unicode string",
			key:   "unicode",
			value: "😄🚀🎉",
			expected: map[string]any{
				"unicode": "😄🚀🎉",
			},
		},
		{
			name:  "string with special chars",
			key:   "special",
			value: "hello\nworld\r\t\b\f",
			expected: map[string]any{
				"special": "hello\nworld\r\t\b\f",
			},
		},
		{
			name:  "string with control chars",
			key:   "control",
			value: "hello\x00world\x07test",
			expected: map[string]any{
				"control": "hello\x00world\x07test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = String(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "String() should not allocate memory")

			// Test output format
			param := String(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestInt64(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    int64
		expected map[string]any
	}{
		{
			name:  "positive int64",
			key:   "count",
			value: 123456789,
			expected: map[string]any{
				"count": 123456789.0,
			},
		},
		{
			name:  "negative int64",
			key:   "negative",
			value: -987654321,
			expected: map[string]any{
				"negative": -987654321.0,
			},
		},
		{
			name:  "zero int64",
			key:   "zero",
			value: 0,
			expected: map[string]any{
				"zero": 0.0,
			},
		},
		{
			name:  "max int64",
			key:   "max",
			value: 9223372036854775807,
			expected: map[string]any{
				"max": 9223372036854775807.0,
			},
		},
		{
			name:  "min int64",
			key:   "min",
			value: -9223372036854775808,
			expected: map[string]any{
				"min": -9223372036854775808.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Int64(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Int64() should not allocate memory")

			// Test output format
			param := Int64(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestInt(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    int
		expected map[string]any
	}{
		{
			name:  "positive int",
			key:   "count",
			value: 42,
			expected: map[string]any{
				"count": 42.0,
			},
		},
		{
			name:  "negative int",
			key:   "negative",
			value: -100,
			expected: map[string]any{
				"negative": -100.0,
			},
		},
		{
			name:  "zero int",
			key:   "zero",
			value: 0,
			expected: map[string]any{
				"zero": 0.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Int(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Int() should not allocate memory")

			// Test output format
			param := Int(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestInt32(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    int32
		expected map[string]any
	}{
		{
			name:  "positive int32",
			key:   "count",
			value: 2147483647,
			expected: map[string]any{
				"count": 2147483647.0,
			},
		},
		{
			name:  "negative int32",
			key:   "negative",
			value: -2147483648,
			expected: map[string]any{
				"negative": -2147483648.0,
			},
		},
		{
			name:  "zero int32",
			key:   "zero",
			value: 0,
			expected: map[string]any{
				"zero": 0.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Int32(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Int32() should not allocate memory")

			// Test output format
			param := Int32(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBool(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    bool
		expected map[string]any
	}{
		{
			name:  "true bool",
			key:   "enabled",
			value: true,
			expected: map[string]any{
				"enabled": true,
			},
		},
		{
			name:  "false bool",
			key:   "disabled",
			value: false,
			expected: map[string]any{
				"disabled": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Bool(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Bool() should not allocate memory")

			// Test output format
			param := Bool(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTime(t *testing.T) {
	now := clock.Now()
	utcTime := time.Date(2023, 12, 25, 15, 30, 45, 123456789, time.UTC)
	zeroTime := time.Time{}

	tests := []struct {
		name     string
		key      string
		value    time.Time
		expected map[string]any
	}{
		{
			name:  "current time",
			key:   "now",
			value: now,
			expected: map[string]any{
				"now": now.UTC().Format("2006-01-02T15:04:05.000000Z"),
			},
		},
		{
			name:  "UTC time",
			key:   "utc",
			value: utcTime,
			expected: map[string]any{
				"utc": "2023-12-25T15:30:45.123456Z",
			},
		},
		{
			name:  "zero time",
			key:   "zero",
			value: zeroTime,
			expected: map[string]any{
				"zero": "0001-01-01T00:00:00.000000Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Time(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Time() should not allocate memory")

			// Test output format
			param := Time(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestError(t *testing.T) {
	err1 := errors.Errorf("test error")
	err2 := errors.Errorf("another error")

	var nilErr error

	tests := []struct {
		name     string
		key      string
		value    error
		expected map[string]any
	}{
		{
			name:  "simple error",
			key:   "err",
			value: err1,
			expected: map[string]any{
				"err": "test error",
			},
		},
		{
			name:  "another error",
			key:   "error",
			value: err2,
			expected: map[string]any{
				"error": "another error",
			},
		},
		{
			name:  "nil error",
			key:   "nil",
			value: nilErr,
			expected: map[string]any{
				"nil": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Error(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Error() should not allocate memory")

			// Test output format
			param := Error(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestUInt64(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    uint64
		expected map[string]any
	}{
		{
			name:  "positive uint64",
			key:   "count",
			value: 18446744073709551615,
			expected: map[string]any{
				"count": 18446744073709551615.0,
			},
		},
		{
			name:  "zero uint64",
			key:   "zero",
			value: 0,
			expected: map[string]any{
				"zero": 0.0,
			},
		},
		{
			name:  "large uint64",
			key:   "large",
			value: 1234567890123456789,
			expected: map[string]any{
				"large": 1234567890123456789.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = UInt64(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "UInt64() should not allocate memory")

			// Test output format
			param := UInt64(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    time.Duration
		expected map[string]any
	}{
		{
			name:  "positive duration",
			key:   "duration",
			value: 5 * time.Second,
			expected: map[string]any{
				"duration": 5000000.0, // microseconds
			},
		},
		{
			name:  "negative duration",
			key:   "negative",
			value: -2 * time.Minute,
			expected: map[string]any{
				"negative": -120000000.0, // microseconds
			},
		},
		{
			name:  "zero duration",
			key:   "zero",
			value: 0,
			expected: map[string]any{
				"zero": 0.0,
			},
		},
		{
			name:  "microsecond duration",
			key:   "micro",
			value: 123 * time.Microsecond,
			expected: map[string]any{
				"micro": 123.0,
			},
		},
		{
			name:  "nanosecond duration",
			key:   "nano",
			value: 500 * time.Nanosecond,
			expected: map[string]any{
				"nano": 0.0, // rounds down to microseconds
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test memory allocations
			allocs := testing.AllocsPerRun(100, func() {
				_ = Duration(tt.key, tt.value)
			})
			require.Equal(t, float64(0), allocs, "Duration() should not allocate memory")

			// Test output format
			param := Duration(tt.key, tt.value)

			jw := contentlog.NewJSONWriter()
			defer jw.Release()

			jw.BeginObject()
			param.WriteValueTo(jw)
			jw.EndObject()

			var result map[string]any

			require.NoError(t, json.Unmarshal(jw.GetBufferForTesting(), &result))
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestWriteValueToMemoryAllocations tests that WriteValueTo methods don't allocate memory.
func TestWriteValueToMemoryAllocations(t *testing.T) {
	jw := contentlog.NewJSONWriter()
	defer jw.Release()

	tests := []struct {
		name  string
		param contentlog.ParamWriter
	}{
		{
			name:  "stringParam",
			param: String("key", "value"),
		},
		{
			name:  "int64Param",
			param: Int64("key", 123),
		},
		{
			name:  "boolParam",
			param: Bool("key", true),
		},
		{
			name:  "timeParam",
			param: Time("key", clock.Now()),
		},
		{
			name:  "errorParam",
			param: Error("key", errors.New("test")),
		},
		{
			name:  "uint64Param",
			param: UInt64("key", 123),
		},
		{
			name:  "durationParam",
			param: Duration("key", time.Second),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				jw.BeginObject()
				tt.param.WriteValueTo(jw)
				jw.EndObject()
			})
			require.Equal(t, float64(0), allocs, "%s.WriteValueTo() should not allocate memory", tt.name)
		})
	}
}
