package cli

import (
	"runtime"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/kopialogging"
)

var trackMemoryUsage = app.Flag("track-memory-usage", "Periodically force GC and log current memory usage").Hidden().Duration()

var memlog = kopialogging.Logger("kopia/memory")

var (
	memoryTrackerMutex            sync.Mutex
	lastHeapUsage, lastStackInUse uint64
	maxHeapUsage, maxStackInUse   uint64
)

func dumpMemoryUsage() {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	memoryTrackerMutex.Lock()
	defer memoryTrackerMutex.Unlock()
	memlog.Debugf("in use heap %v (delta %v max %v) stack %v (delta %v max %v)", ms.HeapInuse, int64(ms.HeapInuse-lastHeapUsage), maxHeapUsage, ms.StackInuse, int64(ms.StackInuse-lastStackInUse), maxStackInUse)
	if ms.HeapInuse > maxHeapUsage {
		maxHeapUsage = ms.HeapInuse
	}
	if ms.StackInuse > maxStackInUse {
		maxStackInUse = ms.StackInuse
	}
	lastHeapUsage = ms.HeapInuse
	lastStackInUse = ms.StackInuse
}

func startMemoryTracking() {
	if *trackMemoryUsage > 0 {
		go func() {
			for {
				dumpMemoryUsage()
				time.Sleep(*trackMemoryUsage)
			}
		}()
	}
}

func finishMemoryTracking() {
	dumpMemoryUsage()
}
