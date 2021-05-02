package cli

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/logging"
)

type memoryTracker struct {
	trackMemoryUsage time.Duration

	memoryTrackerMutex            sync.Mutex
	lastHeapUsage, lastStackInUse uint64
	maxHeapUsage, maxStackInUse   uint64
}

func (c *memoryTracker) setup(app *kingpin.Application) {
	app.Flag("track-memory-usage", "Periodically force GC and log current memory usage").Hidden().DurationVar(&c.trackMemoryUsage)
}

var memlog = logging.GetContextLoggerFunc("kopia/memory")

func (c *memoryTracker) dumpMemoryUsage(ctx context.Context) {
	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)

	c.memoryTrackerMutex.Lock()
	defer c.memoryTrackerMutex.Unlock()
	memlog(ctx).Debugf("in use heap %v (delta %v max %v) stack %v (delta %v max %v)",
		ms.HeapInuse, int64(ms.HeapInuse-c.lastHeapUsage), c.maxHeapUsage, ms.StackInuse, int64(ms.StackInuse-c.lastStackInUse), c.maxStackInUse)

	if ms.HeapInuse > c.maxHeapUsage {
		c.maxHeapUsage = ms.HeapInuse
	}

	if ms.StackInuse > c.maxStackInUse {
		c.maxStackInUse = ms.StackInuse
	}

	c.lastHeapUsage = ms.HeapInuse
	c.lastStackInUse = ms.StackInuse
}

func (c *memoryTracker) startMemoryTracking(ctx context.Context) {
	if c.trackMemoryUsage > 0 {
		go func() {
			for {
				c.dumpMemoryUsage(ctx)
				time.Sleep(c.trackMemoryUsage)
			}
		}()
	}
}

func (c *memoryTracker) finishMemoryTracking(ctx context.Context) {
	c.dumpMemoryUsage(ctx)
}
