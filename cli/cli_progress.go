package cli

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/fatih/color"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	spinner = `|/-\`
)

type progressFlags struct {
	enableProgress              bool
	progressEstimationType      string
	adaptiveEstimationThreshold int64
	progressUpdateInterval      time.Duration
	out                         textOutput
}

func (p *progressFlags) setup(svc appServices, app *kingpin.Application) {
	app.Flag("progress", "Enable progress bar").Hidden().Default("true").BoolVar(&p.enableProgress)
	app.Flag("progress-estimation-type", "Set type of estimation of the data to be snapshotted").Hidden().Default(snapshotfs.EstimationTypeClassic).
		EnumVar(&p.progressEstimationType, snapshotfs.EstimationTypeClassic, snapshotfs.EstimationTypeRough, snapshotfs.EstimationTypeAdaptive)
	app.Flag("progress-update-interval", "How often to update progress information").Hidden().Default("300ms").DurationVar(&p.progressUpdateInterval)
	app.Flag("adaptive-estimation-threshold", "Sets the threshold below which the classic estimation method will be used").Hidden().Default(strconv.FormatInt(snapshotfs.AdaptiveEstimationThreshold, 10)).Int64Var(&p.adaptiveEstimationThreshold)
	p.out.setup(svc)
}

type cliProgress struct {
	snapshotfs.NullUploadProgress

	// all int64 must precede all int32 due to alignment requirements on ARM
	uploadedBytes     atomic.Int64
	cachedBytes       atomic.Int64
	hashedBytes       atomic.Int64
	outputThrottle    timetrack.Throttle
	cachedFiles       atomic.Int32
	inProgressHashing atomic.Int32
	hashedFiles       atomic.Int32
	uploadedFiles     atomic.Int32
	ignoredErrorCount atomic.Int32
	fatalErrorCount   atomic.Int32
	uploading         atomic.Bool
	uploadFinished    atomic.Bool

	outputMutex sync.Mutex

	// +checklocks:outputMutex
	lastLineLength int
	// +checklocks:outputMutex
	spinPhase int

	uploadStartTime timetrack.Estimator // +checklocksignore

	estimatedFileCount  int64 // +checklocksignore
	estimatedTotalBytes int64 // +checklocksignore

	// indicates shared instance that does not reset counters at the beginning of upload.
	shared bool

	progressFlags
}

// Enabled returns true when progress is enabled.
func (p *cliProgress) Enabled() bool {
	return p.enableProgress
}

func (p *cliProgress) HashingFile(_ string) {
	p.inProgressHashing.Add(1)
}

func (p *cliProgress) FinishedHashingFile(_ string, _ int64) {
	p.hashedFiles.Add(1)
	p.inProgressHashing.Add(-1)
	p.maybeOutput()
}

func (p *cliProgress) UploadedBytes(numBytes int64) {
	p.uploadedBytes.Add(numBytes)
	p.uploadedFiles.Add(1)

	p.maybeOutput()
}

func (p *cliProgress) HashedBytes(numBytes int64) {
	p.hashedBytes.Add(numBytes)
	p.maybeOutput()
}

func (p *cliProgress) Error(path string, err error, isIgnored bool) {
	if isIgnored {
		p.ignoredErrorCount.Add(1)
		p.output(warningColor, fmt.Sprintf("Ignored error when processing \"%v\": %v\n", path, err))
	} else {
		p.fatalErrorCount.Add(1)
		p.output(errorColor, fmt.Sprintf("Error when processing \"%v\": %v\n", path, err))
	}
}

func (p *cliProgress) CachedFile(_ string, numBytes int64) {
	p.cachedBytes.Add(numBytes)
	p.cachedFiles.Add(1)
	p.maybeOutput()
}

func (p *cliProgress) maybeOutput() {
	if !p.uploading.Load() {
		return
	}

	if p.outputThrottle.ShouldOutput(p.progressUpdateInterval) {
		p.output(defaultColor, "")
	}
}

func (p *cliProgress) output(col *color.Color, msg string) {
	p.outputMutex.Lock()
	defer p.outputMutex.Unlock()

	hashedBytes := p.hashedBytes.Load()
	cachedBytes := p.cachedBytes.Load()
	uploadedBytes := p.uploadedBytes.Load()
	cachedFiles := p.cachedFiles.Load()
	inProgressHashing := p.inProgressHashing.Load()
	hashedFiles := p.hashedFiles.Load()
	ignoredErrorCount := p.ignoredErrorCount.Load()
	fatalErrorCount := p.fatalErrorCount.Load()

	line := fmt.Sprintf(
		" %v %v hashing, %v hashed (%v), %v cached (%v), uploaded %v",
		p.spinnerCharacter(),

		inProgressHashing,

		hashedFiles,
		units.BytesString(hashedBytes),

		cachedFiles,
		units.BytesString(cachedBytes),

		units.BytesString(uploadedBytes),
	)

	if fatalErrorCount > 0 {
		line += fmt.Sprintf(" (%v fatal errors)", fatalErrorCount)
	}

	if ignoredErrorCount > 0 {
		line += fmt.Sprintf(" (%v errors ignored)", ignoredErrorCount)
	}

	if msg != "" {
		prefix := "\n ! "
		if !p.enableProgress {
			prefix = ""
		}

		col.Fprintf(p.out.stderr(), "%v%v", prefix, msg) //nolint:errcheck
	}

	if !p.enableProgress {
		return
	}

	if est, ok := p.uploadStartTime.Estimate(float64(hashedBytes+cachedBytes), float64(p.estimatedTotalBytes)); ok {
		line += fmt.Sprintf(", estimated %v", units.BytesString(p.estimatedTotalBytes))
		line += fmt.Sprintf(" (%.1f%%)", est.PercentComplete)
		line += fmt.Sprintf(" %v left", est.Remaining)
	} else {
		line += ", estimating..."
	}

	var extraSpaces string

	if len(line) < p.lastLineLength {
		// add extra spaces to wipe over previous line if it was longer than current
		extraSpaces = strings.Repeat(" ", p.lastLineLength-len(line))
	}

	p.lastLineLength = len(line)
	p.out.printStderr("\r%v%v", line, extraSpaces)
}

// +checklocks:p.outputMutex
func (p *cliProgress) spinnerCharacter() string {
	if p.uploadFinished.Load() {
		return "*"
	}

	x := p.spinPhase % len(spinner)
	s := spinner[x : x+1]
	p.spinPhase = (p.spinPhase + 1) % len(spinner)

	return s
}

// +checklocksignore.
func (p *cliProgress) StartShared() {
	*p = cliProgress{
		uploadStartTime: timetrack.Start(),
		shared:          true,
		progressFlags:   p.progressFlags,
	}

	p.uploading.Store(true)
}

func (p *cliProgress) FinishShared() {
	p.uploadFinished.Store(true)
	p.output(defaultColor, "")
}

// +checklocksignore.
func (p *cliProgress) UploadStarted() {
	if p.shared {
		// do nothing
		return
	}

	*p = cliProgress{
		uploadStartTime: timetrack.Start(),
		progressFlags:   p.progressFlags,
	}

	p.uploading.Store(true)
}

func (p *cliProgress) EstimatedDataSize(fileCount, totalBytes int64) {
	if p.shared {
		// do nothing
		return
	}

	p.outputMutex.Lock()
	defer p.outputMutex.Unlock()

	p.estimatedFileCount = fileCount
	p.estimatedTotalBytes = totalBytes
}

func (p *cliProgress) UploadFinished() {
	// do nothing here, we still want to report the files flushed after the Upload has completed.
	// instead, Finish() will be called.
}

func (p *cliProgress) Finish() {
	if p.shared {
		return
	}

	p.uploadFinished.Store(true)
	p.uploading.Store(false)

	p.output(defaultColor, "")

	if p.enableProgress {
		p.out.printStderr("\n")
	}
}

func (p *cliProgress) EstimationParameters() snapshotfs.EstimationParameters {
	return snapshotfs.EstimationParameters{
		Type:              p.progressEstimationType,
		AdaptiveThreshold: p.adaptiveEstimationThreshold,
	}
}

var _ snapshotfs.UploadProgress = (*cliProgress)(nil)
