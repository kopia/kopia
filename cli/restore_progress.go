package cli

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot/restore"
)

type cliRestoreProgress struct {
	restoredCount      atomic.Int32
	enqueuedCount      atomic.Int32
	skippedCount       atomic.Int32
	ignoredErrorsCount atomic.Int32

	restoredTotalFileSize atomic.Int64
	enqueuedTotalFileSize atomic.Int64
	skippedTotalFileSize  atomic.Int64

	progressUpdateInterval time.Duration
	enableProgress         bool

	outputThrottle timetrack.Throttle
	outputMutex    sync.Mutex
	out            textOutput          // +checklocksignore: outputMutex just happens to be held always.
	eta            timetrack.Estimator // +checklocksignore: outputMutex just happens to be held always.

	// +checklocks:outputMutex
	lastLineLength int
}

func (p *cliRestoreProgress) SetCounters(s restore.Stats) {
	p.enqueuedCount.Store(s.EnqueuedFileCount + s.EnqueuedDirCount + s.EnqueuedSymlinkCount)
	p.enqueuedTotalFileSize.Store(s.EnqueuedTotalFileSize)

	p.restoredCount.Store(s.RestoredFileCount + s.RestoredDirCount + s.RestoredSymlinkCount)
	p.restoredTotalFileSize.Store(s.RestoredTotalFileSize)

	p.skippedCount.Store(s.SkippedCount)
	p.skippedTotalFileSize.Store(s.SkippedTotalFileSize)

	p.ignoredErrorsCount.Store(s.IgnoredErrorCount)

	p.maybeOutput()
}

func (p *cliRestoreProgress) Flush() {
	p.outputThrottle.Reset()
	p.output("\n")
}

func (p *cliRestoreProgress) maybeOutput() {
	if p.outputThrottle.ShouldOutput(p.progressUpdateInterval) {
		p.output("")
	}
}

func (p *cliRestoreProgress) output(suffix string) {
	if !p.enableProgress {
		return
	}

	// ensure the counters are not going back in an output line compared to the previous one
	p.outputMutex.Lock()
	defer p.outputMutex.Unlock()

	restoredCount := p.restoredCount.Load()
	enqueuedCount := p.enqueuedCount.Load()
	skippedCount := p.skippedCount.Load()
	ignoredCount := p.ignoredErrorsCount.Load()

	restoredSize := p.restoredTotalFileSize.Load()
	enqueuedSize := p.enqueuedTotalFileSize.Load()
	skippedSize := p.skippedTotalFileSize.Load()

	if restoredSize == 0 {
		return
	}

	var maybeRemaining, maybeSkipped, maybeErrors string
	if est, ok := p.eta.Estimate(float64(restoredSize), float64(enqueuedSize)); ok {
		maybeRemaining = fmt.Sprintf(" %v (%.1f%%) remaining %v",
			units.BytesPerSecondsString(est.SpeedPerSecond),
			est.PercentComplete,
			est.Remaining)
	}

	if skippedCount > 0 {
		maybeSkipped = fmt.Sprintf(", skipped %v (%v)", skippedCount, units.BytesString(skippedSize))
	}

	if ignoredCount > 0 {
		maybeErrors = fmt.Sprintf(", ignored %v errors", ignoredCount)
	}

	line := fmt.Sprintf("Processed %v (%v) of %v (%v)%v%v%v.",
		restoredCount+skippedCount, units.BytesString(restoredSize),
		enqueuedCount, units.BytesString(enqueuedSize),
		maybeSkipped, maybeErrors, maybeRemaining,
	)

	var extraSpaces string

	if len(line) < p.lastLineLength {
		// add extra spaces to wipe over previous line if it was longer than current
		extraSpaces = strings.Repeat(" ", p.lastLineLength-len(line))
	}

	p.lastLineLength = len(line)
	p.out.printStderr("\r%v%v%v", line, extraSpaces, suffix)
}
