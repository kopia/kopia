// Package logparam provides parameters for logging.
package logparam

import (
	"time"

	"github.com/kopia/kopia/internal/contentlog"
)

// String creates a string parameter.
//
//nolint:revive
func String(key, value string) stringParam {
	return stringParam{Key: key, Value: value}
}

// Int64 creates an int64 parameter.
//
//nolint:revive
func Int64(key string, value int64) int64Param { return int64Param{Key: key, Value: value} }

// Int creates an int parameter.
//
//nolint:revive
func Int(key string, value int) int64Param {
	return int64Param{Key: key, Value: int64(value)}
}

// Int32 creates an int32 parameter.
//
//nolint:revive
func Int32(key string, value int32) int64Param {
	return int64Param{Key: key, Value: int64(value)}
}

// Bool creates a bool parameter.
//
//nolint:revive
func Bool(key string, value bool) boolParam { return boolParam{Key: key, Value: value} }

// Time creates a time parameter.
//
//nolint:revive
func Time(key string, value time.Time) timeParam { return timeParam{Key: key, Value: value} }

// Error creates an error parameter.
//
//nolint:revive
func Error(key string, value error) errorParam { return errorParam{Key: key, Value: value} }

// UInt64 creates a uint64 parameter.
//
//nolint:revive
func UInt64(key string, value uint64) uint64Param {
	return uint64Param{Key: key, Value: value}
}

// UInt32 creates a uint32 parameter.
//
//nolint:revive
func UInt32(key string, value uint32) uint64Param {
	return uint64Param{Key: key, Value: uint64(value)}
}

// Duration creates a duration parameter.
//
//nolint:revive
func Duration(key string, value time.Duration) durationParam {
	return durationParam{Key: key, Value: value}
}

// int64Param is a parameter that writes a int64 value to the JSON writer.
type int64Param struct {
	Key   string
	Value int64
}

func (v int64Param) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.Int64Field(v.Key, v.Value)
}

type uint64Param struct {
	Key   string
	Value uint64
}

func (v uint64Param) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.UInt64Field(v.Key, v.Value)
}

type timeParam struct {
	Key   string
	Value time.Time
}

func (v timeParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.TimeField(v.Key, v.Value)
}

type boolParam struct {
	Key   string
	Value bool
}

func (v boolParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.BoolField(v.Key, v.Value)
}

type durationParam struct {
	Key   string
	Value time.Duration
}

func (v durationParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.Int64Field(v.Key, v.Value.Microseconds())
}

type errorParam struct {
	Key   string
	Value error
}

func (v errorParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.ErrorField(v.Key, v.Value)
}

type stringParam struct {
	Key   string
	Value string
}

func (v stringParam) WriteValueTo(jw *contentlog.JSONWriter) {
	jw.StringField(v.Key, v.Value)
}
