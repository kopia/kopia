package cli

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
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

	svc            appServices
	outputThrottle timetrack.Throttle
	outputMutex    sync.Mutex
	out            textOutput
	eta            timetrack.Estimator

	// +checklocks:outputMutex
	lastLineLength int
}

func (p *cliRestoreProgress) setup(svc appServices, _ *kingpin.Application) {
	cp := svc.getProgress()
	if cp == nil {
		return
	}

	p.progressUpdateInterval = cp.progressUpdateInterval
	p.enableProgress = cp.enableProgress
	p.out = cp.out
	p.svc = svc

	p.eta = timetrack.Start()
}

func (p *cliRestoreProgress) SetCounters(
	enqueuedCount, restoredCount, skippedCount, ignoredErrors int32,
	enqueuedBytes, restoredBytes, skippedBytes int64,
) {
	p.enqueuedCount.Store(enqueuedCount)
	p.enqueuedTotalFileSize.Store(enqueuedBytes)

	p.restoredCount.Store(restoredCount)
	p.restoredTotalFileSize.Store(restoredBytes)

	p.skippedCount.Store(skippedCount)
	p.skippedTotalFileSize.Store(skippedBytes)

	p.ignoredErrorsCount.Store(ignoredErrors)

	p.maybeOutput()
}

func (p *cliRestoreProgress) Flush() {
	p.outputThrottle.Reset()
	p.output("\n")
}

func (p *cliRestoreProgress) maybeOutput() {
	if p.outputThrottle.ShouldOutput(p.svc.getProgress().progressUpdateInterval) {
		p.output("")
	}
}

func (p *cliRestoreProgress) output(suffix string) {
	if !p.svc.getProgress().enableProgress {
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
