package contentlog_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
)

func TestNewLogger(t *testing.T) {
	t.Run("creates logger with output function", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		require.NotNil(t, logger)
	})

	t.Run("creates logger with parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		params := []contentlog.ParamWriter{
			logparam.String("service", "test"),
			logparam.Int("version", 1),
		}

		logger := contentlog.NewLogger(outputFunc, params...)
		require.NotNil(t, logger)
	})

	t.Run("creates logger with nil output", func(t *testing.T) {
		logger := contentlog.NewLogger(nil)
		require.NotNil(t, logger)
	})
}

func TestLog(t *testing.T) {
	t.Run("logs message with no parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log(ctx, logger, "test message")

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "test message", logEntry["m"])
		require.Contains(t, logEntry, "t") // timestamp field
	})

	t.Run("logs message with logger parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		params := []contentlog.ParamWriter{
			logparam.String("service", "test-service"),
			logparam.Int("version", 2),
		}

		logger := contentlog.NewLogger(outputFunc, params...)
		ctx := context.Background()

		contentlog.Log(ctx, logger, "test message")

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "test message", logEntry["m"])
		require.Equal(t, "test-service", logEntry["service"])
		require.Equal(t, float64(2), logEntry["version"])
		require.Contains(t, logEntry, "t") // timestamp field
	})

	t.Run("handles nil logger gracefully", func(t *testing.T) {
		ctx := context.Background()
		// This should not panic
		contentlog.Log(ctx, nil, "test message")
	})

	t.Run("handles nil output gracefully", func(t *testing.T) {
		logger := contentlog.NewLogger(nil)
		ctx := context.Background()
		// This should not panic
		contentlog.Log(ctx, logger, "test message")
	})
}

func TestLog1(t *testing.T) {
	t.Run("logs message with one parameter", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log1(ctx, logger, "processing item", logparam.String("id", "item-123"))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
	})

	t.Run("logs message with different parameter types", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		testCases := []struct {
			name     string
			message  string
			param    contentlog.ParamWriter
			key      string
			expected any
		}{
			{"string", "string param", logparam.String("str", "hello"), "str", "hello"},
			{"int", "int param", logparam.Int("num", 42), "num", float64(42)},
			{"int64", "int64 param", logparam.Int64("big", 9223372036854775807), "big", float64(9223372036854775807)},
			{"bool", "bool param", logparam.Bool("flag", true), "flag", true},
			{"uint64", "uint64 param", logparam.UInt64("unsigned", 18446744073709551615), "unsigned", float64(18446744073709551615)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				captured = nil // reset

				contentlog.Log1(ctx, logger, tc.message, tc.param)

				require.NotEmpty(t, captured)

				var logEntry map[string]any
				err := json.Unmarshal(captured, &logEntry)
				require.NoError(t, err)
				require.Equal(t, tc.message, logEntry["m"])
				require.Equal(t, tc.expected, logEntry[tc.key])
			})
		}
	})
}

func TestLog2(t *testing.T) {
	t.Run("logs message with two parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log2(ctx, logger, "processing item",
			logparam.String("id", "item-123"),
			logparam.Int("count", 5))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
		require.Equal(t, float64(5), logEntry["count"])
	})
}

func TestLog3(t *testing.T) {
	t.Run("logs message with three parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log3(ctx, logger, "processing item",
			logparam.String("id", "item-123"),
			logparam.Int("count", 5),
			logparam.Bool("active", true))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
		require.Equal(t, float64(5), logEntry["count"])
		require.Equal(t, true, logEntry["active"])
	})
}

func TestLog4(t *testing.T) {
	t.Run("logs message with four parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log4(ctx, logger, "processing item",
			logparam.String("id", "item-123"),
			logparam.Int("count", 5),
			logparam.Bool("active", true),
			logparam.String("status", "processing"))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
		require.Equal(t, float64(5), logEntry["count"])
		require.Equal(t, true, logEntry["active"])
		require.Equal(t, "processing", logEntry["status"])
	})
}

func TestLog5(t *testing.T) {
	t.Run("logs message with five parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log5(ctx, logger, "processing item",
			logparam.String("id", "item-123"),
			logparam.Int("count", 5),
			logparam.Bool("active", true),
			logparam.String("status", "processing"),
			logparam.Int64("size", 1024))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
		require.Equal(t, float64(5), logEntry["count"])
		require.Equal(t, true, logEntry["active"])
		require.Equal(t, "processing", logEntry["status"])
		require.Equal(t, float64(1024), logEntry["size"])
	})
}

func TestLog6(t *testing.T) {
	t.Run("logs message with six parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log6(ctx, logger, "processing item",
			logparam.String("id", "item-123"),
			logparam.Int("count", 5),
			logparam.Bool("active", true),
			logparam.String("status", "processing"),
			logparam.Int64("size", 1024),
			logparam.UInt64("flags", 0xFF))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "processing item", logEntry["m"])
		require.Equal(t, "item-123", logEntry["id"])
		require.Equal(t, float64(5), logEntry["count"])
		require.Equal(t, true, logEntry["active"])
		require.Equal(t, "processing", logEntry["status"])
		require.Equal(t, float64(1024), logEntry["size"])
		require.Equal(t, float64(0xFF), logEntry["flags"])
	})
}

func TestEmit(t *testing.T) {
	t.Run("emits custom WriterTo entry", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		customEntry := &customLogEntry{
			message: "custom entry",
			value:   42,
		}

		contentlog.Emit(ctx, logger, customEntry)

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "custom entry", logEntry["message"])
		require.Equal(t, float64(42), logEntry["value"])
		require.Contains(t, logEntry, "t") // timestamp field
	})

	t.Run("emits with logger parameters", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		params := []contentlog.ParamWriter{
			logparam.String("service", "custom-service"),
			logparam.Int("version", 3),
		}

		logger := contentlog.NewLogger(outputFunc, params...)
		ctx := context.Background()

		customEntry := &customLogEntry{
			message: "custom entry",
			value:   42,
		}

		contentlog.Emit(ctx, logger, customEntry)

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "custom entry", logEntry["message"])
		require.Equal(t, float64(42), logEntry["value"])
		require.Equal(t, "custom-service", logEntry["service"])
		require.Equal(t, float64(3), logEntry["version"])
	})

	t.Run("handles nil logger gracefully", func(t *testing.T) {
		ctx := context.Background()
		customEntry := &customLogEntry{message: "test", value: 1}
		// This should not panic
		contentlog.Emit(ctx, nil, customEntry)
	})

	t.Run("handles nil output gracefully", func(t *testing.T) {
		logger := contentlog.NewLogger(nil)
		ctx := context.Background()
		customEntry := &customLogEntry{message: "test", value: 1}
		// This should not panic
		contentlog.Emit(ctx, logger, customEntry)
	})
}

func TestLoggerMultipleLogs(t *testing.T) {
	t.Run("handles multiple log entries", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log(ctx, logger, "first message")
		contentlog.Log1(ctx, logger, "second message", logparam.String("id", "123"))
		contentlog.Log2(ctx, logger, "third message", logparam.Int("count", 5), logparam.Bool("flag", true))

		require.NotEmpty(t, captured)

		// Split by newlines to get individual log entries
		lines := strings.Split(strings.TrimSpace(string(captured)), "\n")
		require.Len(t, lines, 3)

		// Check first entry
		var entry1 map[string]any
		err := json.Unmarshal([]byte(lines[0]), &entry1)
		require.NoError(t, err)
		require.Equal(t, "first message", entry1["m"])

		// Check second entry
		var entry2 map[string]any
		err = json.Unmarshal([]byte(lines[1]), &entry2)
		require.NoError(t, err)
		require.Equal(t, "second message", entry2["m"])
		require.Equal(t, "123", entry2["id"])

		// Check third entry
		var entry3 map[string]any
		err = json.Unmarshal([]byte(lines[2]), &entry3)
		require.NoError(t, err)
		require.Equal(t, "third message", entry3["m"])
		require.Equal(t, float64(5), entry3["count"])
		require.Equal(t, true, entry3["flag"])
	})
}

func TestLoggerErrorHandling(t *testing.T) {
	t.Run("handles nil error parameter", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		contentlog.Log1(ctx, logger, "error test", logparam.Error("err", nil))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "error test", logEntry["m"])
		require.Nil(t, logEntry["err"])
	})

	t.Run("handles real error parameter", func(t *testing.T) {
		var captured []byte

		outputFunc := func(data []byte) {
			captured = append(captured, data...)
		}

		logger := contentlog.NewLogger(outputFunc)
		ctx := context.Background()

		testErr := &testError{msg: "operation failed"}
		contentlog.Log1(ctx, logger, "error test", logparam.Error("err", testErr))

		require.NotEmpty(t, captured)

		var logEntry map[string]any
		err := json.Unmarshal(captured, &logEntry)
		require.NoError(t, err)
		require.Equal(t, "error test", logEntry["m"])
		require.Equal(t, "operation failed", logEntry["err"])
	})
}

// Helper types for testing

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

type customLogEntry struct {
	message string
	value   int
}

func (e *customLogEntry) WriteTo(jw *contentlog.JSONWriter) {
	jw.StringField("message", e.message)
	jw.Int64Field("value", int64(e.value))
}
