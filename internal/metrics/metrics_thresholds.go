package metrics

import (
	"time"

	"golang.org/x/exp/constraints"
)

// Thresholds encapsulates a set of bucket thresholds used in Summary[T].
type Thresholds[T constraints.Float | constraints.Integer] struct {
	values           []T
	promScale        float64
	prometheusSuffix string
}

// ISOBytesThresholds is a set of thresholds for sizes.
//
//nolint:gochecknoglobals
var ISOBytesThresholds = &Thresholds[int64]{
	[]int64{
		100,
		250,
		500,
		1_000,
		2_500,
		5_000,
		10_000,
		25_000,
		50_000,
		100_000,
		250_000,
		500_000,
		1_000_000,
		2_500_000,
		5_000_000,
		10_000_000,
		25_000_000,
		50_000_000,
		100_000_000,
		250_000_000,
		500_000_000,
	},
	1,
	"",
}

// IOLatencyThresholds is a set of thresholds that can represent IO latencies from 500us to 50s.
//
//nolint:gochecknoglobals
var IOLatencyThresholds = &Thresholds[time.Duration]{
	[]time.Duration{
		500 * time.Microsecond,

		1000 * time.Microsecond,
		2500 * time.Microsecond,
		5000 * time.Microsecond,

		10 * time.Millisecond,
		25 * time.Millisecond,
		50 * time.Millisecond,

		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,

		1000 * time.Millisecond,
		2500 * time.Millisecond,
		5000 * time.Millisecond,

		10 * time.Second,
		25 * time.Second,
		50 * time.Second,
	},
	1e6, // store as milliseconds
	"_ms",
}

// CPULatencyThresholds is a set of thresholds that can represent CPU latencies from 10us to 5s.
//
//nolint:gochecknoglobals
var CPULatencyThresholds = &Thresholds[time.Duration]{
	[]time.Duration{
		10 * time.Microsecond,
		25 * time.Microsecond,
		50 * time.Microsecond,

		100 * time.Microsecond,
		250 * time.Microsecond,
		500 * time.Microsecond,

		1000 * time.Microsecond,
		2500 * time.Microsecond,
		5000 * time.Microsecond,

		10 * time.Millisecond,
		25 * time.Millisecond,
		50 * time.Millisecond,

		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,

		1000 * time.Millisecond,
		2500 * time.Millisecond,
		5000 * time.Millisecond,
	},
	1, // export to Prometheus as nanoseconds
	"_ns",
}

func bucketForThresholds[T constraints.Integer | constraints.Float](thresholds []T, d T) int {
	l, r := 0, len(thresholds)-1

	for l <= r {
		m := (l + r) >> 1
		mid := thresholds[m]

		switch {
		case mid < d:
			l = m + 1
		case mid > d:
			r = m - 1
		default:
			return m
		}
	}

	return l
}
