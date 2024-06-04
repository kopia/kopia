// Package zaplogutil provides reusable utilities for working with ZAP logger.
package zaplogutil

import (
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"

	"github.com/kopia/kopia/internal/clock"
)

// PreciseLayout is a variant of time.RFC3339Nano but with microsecond precision
// and trailing zeroes.
const PreciseLayout = "2006-01-02T15:04:05.000000Z07:00"

// PreciseTimeEncoder encodes the time as RFC3389 with 6 digits of sub-second precision.
func PreciseTimeEncoder() zapcore.TimeEncoder {
	return zapcore.TimeEncoderOfLayout(PreciseLayout)
}

type theClock struct{}

func (c theClock) Now() time.Time                         { return clock.Now() }
func (c theClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// Clock is an implementation of zapcore.Clock that uses clock.Now().
func Clock() zapcore.Clock {
	return theClock{}
}

// TimezoneAdjust returns zapcore.TimeEncoder that adjusts the time to either UTC or local time before logging.
func TimezoneAdjust(inner zapcore.TimeEncoder, isLocal bool) zapcore.TimeEncoder {
	if isLocal {
		return func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
			inner(t.Local(), pae)
		}
	}

	return func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
		inner(t.UTC(), pae)
	}
}

// NewStdConsoleEncoder returns standardized console encoder which is optimized
// for performance.
func NewStdConsoleEncoder(ec StdConsoleEncoderConfig) zapcore.Encoder {
	return &stdConsoleEncoder{zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		SkipLineEnding: true,
	}), ec}
}

// StdConsoleEncoderConfig provides configurationfor NewStdConsoleEncoder.
type StdConsoleEncoderConfig struct {
	TimeLayout         string
	LocalTime          bool
	EmitLoggerName     bool
	EmitLogLevel       bool
	DoNotEmitInfoLevel bool
	ColoredLogLevel    bool
}

//nolint:gochecknoglobals
var bufPool = buffer.NewPool()

type stdConsoleEncoder struct {
	zapcore.Encoder // inherit JSON encoder

	StdConsoleEncoderConfig
}

func (c *stdConsoleEncoder) Clone() zapcore.Encoder {
	return &stdConsoleEncoder{
		c.Encoder.Clone(),
		c.StdConsoleEncoderConfig,
	}
}

func (c *stdConsoleEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := bufPool.Get()

	separator := ""

	if c.TimeLayout != "" {
		if c.LocalTime {
			line.AppendTime(ent.Time.Local(), c.TimeLayout)
		} else {
			line.AppendTime(ent.Time.UTC(), c.TimeLayout)
		}

		separator = " "
	}

	if c.EmitLogLevel {
		line.AppendString(separator)

		if ent.Level != zapcore.InfoLevel || !c.DoNotEmitInfoLevel {
			if c.ColoredLogLevel {
				switch ent.Level {
				case zapcore.DebugLevel:
					line.AppendString("\x1b[35m") // magenta
				case zapcore.WarnLevel:
					line.AppendString("\x1b[33m") // yellow
				default:
					line.AppendString("\x1b[31m") // red
				}

				line.AppendString(ent.Level.CapitalString())
				line.AppendString("\x1b[0m")
			} else {
				line.AppendString(ent.Level.CapitalString())
			}

			separator = " "
		}
	}

	if ent.LoggerName != "" && c.EmitLoggerName {
		line.AppendString(separator)
		line.AppendString(ent.LoggerName)

		separator = " "
	}

	line.AppendString(separator)
	line.AppendString(ent.Message)

	if line2, err := c.Encoder.EncodeEntry(ent, fields); err == nil {
		if line2.Len() > 2 { //nolint:mnd
			line.AppendString("\t")
			line.AppendString(line2.String())
		}

		line2.Free()
	}

	line.AppendString("\n")

	return line, nil
}
