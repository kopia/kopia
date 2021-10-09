// Package memtrack implements utility to log memory usage.
package memtrack

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("memtrack")

type tracker struct {
	name                                  string
	memoryTrackerMutex                    sync.Mutex
	initialMemStats                       runtime.MemStats
	previousMemStats                      runtime.MemStats
	maxAlloc, maxHeapUsage, maxStackInUse uint64
}

func (c *tracker) dump(ctx context.Context, desc string) {
	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)

	c.memoryTrackerMutex.Lock()
	defer c.memoryTrackerMutex.Unlock()

	if ms.HeapInuse > c.maxHeapUsage {
		c.maxHeapUsage = ms.HeapInuse
	}

	if ms.StackInuse > c.maxStackInUse {
		c.maxStackInUse = ms.StackInuse
	}

	if ms.Alloc > c.maxAlloc {
		c.maxAlloc = ms.Alloc
	}

	log(ctx).Debugf(
		"%v: %v allocated %v%v max %v, sys: %v total %v%v, allocs %v%v frees %v%v alive %v%v, goroutines %v",
		c.name,
		desc,

		ms.Alloc-c.initialMemStats.Alloc,
		deltaString(ms.Alloc, c.previousMemStats.Alloc),
		c.maxAlloc,

		ms.HeapSys,

		ms.TotalAlloc-c.initialMemStats.TotalAlloc,
		deltaString(ms.TotalAlloc, c.previousMemStats.TotalAlloc),

		ms.Mallocs-c.initialMemStats.Mallocs,
		deltaString(ms.Mallocs, c.previousMemStats.Mallocs),

		ms.Frees-c.initialMemStats.Frees,
		deltaString(ms.Frees, c.previousMemStats.Frees),

		ms.Mallocs-ms.Frees,
		deltaString(ms.Mallocs-ms.Frees, c.previousMemStats.Mallocs-c.previousMemStats.Frees),

		runtime.NumGoroutine(),
	)

	c.previousMemStats = ms
}

type trackerKey struct{}

// Attach creates a child context with a given tracker attached.
func Attach(ctx context.Context, name string) context.Context {
	v := ctx.Value(trackerKey{})
	if v != nil {
		name = v.(*tracker).name + "::" + name
	}

	t := &tracker{name: name}
	runtime.ReadMemStats(&t.initialMemStats)

	return context.WithValue(ctx, trackerKey{}, t)
}

func deltaString(cur, prev uint64) string {
	if cur == prev {
		return ""
	}

	if cur > prev {
		return fmt.Sprintf("(+%v)", cur-prev)
	}

	return fmt.Sprintf("(-%v)", prev-cur)
}

// Dump logs memory usage if the current context is associated with a tracker.
func Dump(ctx context.Context, desc string) {
	v := ctx.Value(trackerKey{})
	if v == nil {
		return
	}

	v.(*tracker).dump(ctx, desc)
}
