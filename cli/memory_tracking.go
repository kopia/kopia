package cli

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/logging"
)

var trackMemoryUsage = app.Flag("track-memory-usage", "Periodically force GC and log current memory usage").Hidden().Duration()

var memlog = logging.GetContextLoggerFunc("kopia/memory")

var (
	memoryTrackerMutex            sync.Mutex
	lastHeapUsage, lastStackInUse uint64
	maxHeapUsage, maxStackInUse   uint64
)

func dumpMemoryUsage(ctx context.Context) {
	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)

	memoryTrackerMutex.Lock()
	defer memoryTrackerMutex.Unlock()
	memlog(ctx).Debugf("in use heap %v (delta %v max %v) stack %v (delta %v max %v)", ms.HeapInuse, int64(ms.HeapInuse-lastHeapUsage), maxHeapUsage, ms.StackInuse, int64(ms.StackInuse-lastStackInUse), maxStackInUse)

	if ms.HeapInuse > maxHeapUsage {
		maxHeapUsage = ms.HeapInuse
	}

	if ms.StackInuse > maxStackInUse {
		maxStackInUse = ms.StackInuse
	}

	lastHeapUsage = ms.HeapInuse
	lastStackInUse = ms.StackInuse
}

func startMemoryTracking(ctx context.Context) {
	if *trackMemoryUsage > 0 {
		go func() {
			for {
				dumpMemoryUsage(ctx)
				time.Sleep(*trackMemoryUsage)
			}
		}()
	}
}

func finishMemoryTracking(ctx context.Context) {
	dumpMemoryUsage(ctx)
}
