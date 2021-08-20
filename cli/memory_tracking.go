package cli

import (
	"context"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/internal/memtrack"
)

type memoryTracker struct {
	trackMemoryUsage time.Duration
}

func (c *memoryTracker) setup(app *kingpin.Application) {
	app.Flag("track-memory-usage", "Periodically force GC and log current memory usage").Hidden().DurationVar(&c.trackMemoryUsage)
}

func (c *memoryTracker) startMemoryTracking(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = memtrack.Attach(ctx, "memory")

	var (
		closed = make(chan struct{})
		wg     sync.WaitGroup
	)

	if c.trackMemoryUsage > 0 {
		ticker := time.NewTicker(c.trackMemoryUsage)

		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-closed:
					return

				case <-ticker.C:
					memtrack.Dump(ctx, "periodic")
				}
			}
		}()
	}

	return ctx, func() {
		close(closed)
		wg.Wait()

		memtrack.Dump(ctx, "final")
	}
}
