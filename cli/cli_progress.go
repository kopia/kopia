package cli

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/fatih/color"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	spinner = `|/-\`
)

type progressFlags struct {
	enableProgress         bool
	progressUpdateInterval time.Duration
	out                    textOutput
}

func (p *progressFlags) setup(svc appServices, app *kingpin.Application) {
	app.Flag("progress", "Enable progress bar").Hidden().Default("true").BoolVar(&p.enableProgress)
	app.Flag("progress-update-interval", "How often to update progress information").Hidden().Default("300ms").DurationVar(&p.progressUpdateInterval)
	p.out.setup(svc)
}

type cliProgress struct {
	snapshotfs.NullUploadProgress

	// all int64 must precede all int32 due to alignment requirements on ARM
	// +checkatomic
	uploadedBytes int64
	// +checkatomic
	cachedBytes int64
	// +checkatomic
	hashedBytes    int64
	outputThrottle timetrack.Throttle // is int64

	// +checkatomic
	cachedFiles int32
	// +checkatomic
	inProgressHashing int32
	// +checkatomic
	hashedFiles int32
	// +checkatomic
	uploadedFiles int32
	// +checkatomic
	ignoredErrorCount int32
	// +checkatomic
	fatalErrorCount int32

	// +checkatomic
	uploading int32
	// +checkatomic
	uploadFinished int32

	outputMutex sync.Mutex

	// +checklocks:outputMutex
	lastLineLength int
	// +checklocks:outputMutex
	spinPhase int

	uploadStartTime timetrack.Estimator // +checklocksignore

	estimatedFileCount  int   // +checklocksignore
	estimatedTotalBytes int64 // +checklocksignore

	// indicates shared instance that does not reset counters at the beginning of upload.
	shared bool

	progressFlags
}

func (p *cliProgress) HashingFile(fname string) {
	atomic.AddInt32(&p.inProgressHashing, 1)
}

func (p *cliProgress) FinishedHashingFile(fname string, totalSize int64) {
	atomic.AddInt32(&p.hashedFiles, 1)
	atomic.AddInt32(&p.inProgressHashing, -1)
	p.maybeOutput()
}

func (p *cliProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.uploadedBytes, numBytes)
	atomic.AddInt32(&p.uploadedFiles, 1)

	p.maybeOutput()
}

func (p *cliProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.hashedBytes, numBytes)
	p.maybeOutput()
}

func (p *cliProgress) Error(path string, err error, isIgnored bool) {
	if isIgnored {
		atomic.AddInt32(&p.ignoredErrorCount, 1)
		p.output(warningColor, fmt.Sprintf("Ignored error when processing \"%v\": %v\n", path, err))
	} else {
		atomic.AddInt32(&p.fatalErrorCount, 1)
		p.output(warningColor, fmt.Sprintf("Error when processing \"%v\": %v\n", path, err))
	}
}

func (p *cliProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt64(&p.cachedBytes, numBytes)
	atomic.AddInt32(&p.cachedFiles, 1)
	p.maybeOutput()
}

func (p *cliProgress) maybeOutput() {
	if atomic.LoadInt32(&p.uploading) == 0 {
		return
	}

	if p.outputThrottle.ShouldOutput(p.progressUpdateInterval) {
		p.output(defaultColor, "")
	}
}

func (p *cliProgress) output(col *color.Color, msg string) {
	p.outputMutex.Lock()
	defer p.outputMutex.Unlock()

	hashedBytes := atomic.LoadInt64(&p.hashedBytes)
	cachedBytes := atomic.LoadInt64(&p.cachedBytes)
	uploadedBytes := atomic.LoadInt64(&p.uploadedBytes)
	cachedFiles := atomic.LoadInt32(&p.cachedFiles)
	inProgressHashing := atomic.LoadInt32(&p.inProgressHashing)
	hashedFiles := atomic.LoadInt32(&p.hashedFiles)
	ignoredErrorCount := atomic.LoadInt32(&p.ignoredErrorCount)
	fatalErrorCount := atomic.LoadInt32(&p.fatalErrorCount)

	line := fmt.Sprintf(
		" %v %v hashing, %v hashed (%v), %v cached (%v), uploaded %v",
		p.spinnerCharacter(),

		inProgressHashing,

		hashedFiles,
		units.BytesStringBaseEnv(hashedBytes),

		cachedFiles,
		units.BytesStringBaseEnv(cachedBytes),

		units.BytesStringBaseEnv(uploadedBytes),
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
		line += fmt.Sprintf(", estimated %v", units.BytesStringBaseEnv(p.estimatedTotalBytes))
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
	if atomic.LoadInt32(&p.uploadFinished) == 1 {
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
		uploading:       1,
		uploadStartTime: timetrack.Start(),
		shared:          true,
		progressFlags:   p.progressFlags,
	}
}

func (p *cliProgress) FinishShared() {
	atomic.StoreInt32(&p.uploadFinished, 1)
	p.output(defaultColor, "")
}

// +checklocksignore.
func (p *cliProgress) UploadStarted() {
	if p.shared {
		// do nothing
		return
	}

	*p = cliProgress{
		uploading:       1,
		uploadStartTime: timetrack.Start(),
		progressFlags:   p.progressFlags,
	}
}

func (p *cliProgress) EstimatedDataSize(fileCount int, totalBytes int64) {
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

	atomic.StoreInt32(&p.uploadFinished, 1)
	atomic.StoreInt32(&p.uploading, 0)

	p.output(defaultColor, "")

	if p.enableProgress {
		p.out.printStderr("\n")
	}
}

var _ snapshotfs.UploadProgress = (*cliProgress)(nil)
