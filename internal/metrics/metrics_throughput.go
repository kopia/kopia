package metrics

import (
	"time"
)

// Throughput measures throughput by keeping track of total value and total duration.
type Throughput struct {
	totalBytes    *Counter
	totalDuration *Counter
}

// Observe increases duration and total value counters based on the amount of time to process particular amount of data.
func (c *Throughput) Observe(size int64, dt time.Duration) {
	if c == nil {
		return
	}

	c.totalBytes.Add(size)
	c.totalDuration.Add(dt.Nanoseconds())
}

// Throughput gets a persistent counter with the provided name.
func (r *Registry) Throughput(name, help string, labels map[string]string) *Throughput {
	if r == nil {
		return nil
	}

	fullName := name + labelsSuffix(labels)

	c := r.allThroughput[fullName]
	if c == nil {
		c = &Throughput{
			totalBytes:    r.CounterInt64(name+"_bytes", help, labels),
			totalDuration: r.CounterInt64(name+"_duration_nanos", help, labels),
		}

		r.allThroughput[fullName] = c
	}

	return c
}
