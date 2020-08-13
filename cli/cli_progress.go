package cli

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	enableProgress         = app.Flag("progress", "Enable progress bar").Hidden().Default("true").Bool()
	progressUpdateInterval = app.Flag("progress-update-interval", "How ofter to update progress information").Hidden().Default("300ms").Duration()
)

const (
	spinner        = `|/-\`
	hundredPercent = 100.0
)

type cliProgress struct {
	snapshotfs.NullUploadProgress

	// all int64 must precede all int32 due to alignment requirements on ARM
	uploadedBytes          int64
	cachedBytes            int64
	hashedBytes            int64
	nextOutputTimeUnixNano int64

	cachedFiles       int32
	inProgressHashing int32
	hashedFiles       int32
	uploadedFiles     int32
	errorCount        int32

	uploading      int32
	uploadFinished int32

	lastLineLength  int
	spinPhase       int
	uploadStartTime time.Time

	previousFileCount int
	previousTotalSize int64

	// indicates shared instance that does not reset counters at the beginning of upload.
	shared bool

	outputMutex sync.Mutex
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

func (p *cliProgress) IgnoredError(path string, err error) {
	atomic.AddInt32(&p.errorCount, 1)
	p.output(warningColor, fmt.Sprintf("Ignored error when processing \"%v\": %v\n", path, err))
}

func (p *cliProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt64(&p.cachedBytes, numBytes)
	atomic.AddInt32(&p.cachedFiles, 1)
	p.maybeOutput()
}

func (p *cliProgress) Checkpoint() {
	p.output(noticeColor, "Saving a checkpoint...\n")

	if p.shared {
		// do not reset counters
		return
	}

	*p = cliProgress{
		uploading:         1,
		uploadStartTime:   time.Now(),
		previousFileCount: p.previousFileCount,
		previousTotalSize: p.previousTotalSize,
		uploadedBytes:     p.uploadedBytes,
		uploadedFiles:     p.uploadedFiles,
	}
}

func (p *cliProgress) maybeOutput() {
	if atomic.LoadInt32(&p.uploading) == 0 {
		return
	}

	var shouldOutput bool

	nextOutputTimeUnixNano := atomic.LoadInt64(&p.nextOutputTimeUnixNano)
	if nowNano := time.Now().UnixNano(); nowNano > nextOutputTimeUnixNano {
		if atomic.CompareAndSwapInt64(&p.nextOutputTimeUnixNano, nextOutputTimeUnixNano, nowNano+progressUpdateInterval.Nanoseconds()) {
			shouldOutput = true
		}
	}

	if shouldOutput {
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
	uploadedFiles := atomic.LoadInt32(&p.uploadedFiles)
	errorCount := atomic.LoadInt32(&p.errorCount)

	line := fmt.Sprintf(
		" %v %v hashing, %v hashed (%v), %v cached (%v), %v uploaded (%v), %v errors",
		p.spinnerCharacter(),

		inProgressHashing,

		hashedFiles,
		units.BytesStringBase10(hashedBytes),

		cachedFiles,
		units.BytesStringBase10(cachedBytes),

		uploadedFiles,
		units.BytesStringBase10(uploadedBytes),

		errorCount,
	)

	if msg != "" {
		prefix := "\n ! "
		if !*enableProgress {
			prefix = ""
		}

		col.Fprintf(os.Stderr, "%v%v", prefix, msg) // nolint:errcheck
	}

	if !*enableProgress {
		return
	}

	if p.previousTotalSize > 0 {
		percent := (float64(hashedBytes+cachedBytes) * hundredPercent / float64(p.previousTotalSize))
		if percent > hundredPercent {
			percent = hundredPercent
		}

		line += fmt.Sprintf(" %.1f%%", percent)
	}

	var extraSpaces string

	if len(line) < p.lastLineLength {
		// add extra spaces to wipe over previous line if it was longer than current
		extraSpaces = strings.Repeat(" ", p.lastLineLength-len(line))
	}

	p.lastLineLength = len(line)
	printStderr("\r%v%v", line, extraSpaces)
}

func (p *cliProgress) spinnerCharacter() string {
	if atomic.LoadInt32(&p.uploadFinished) == 1 {
		return "*"
	}

	x := p.spinPhase % len(spinner)
	s := spinner[x : x+1]
	p.spinPhase = (p.spinPhase + 1) % len(spinner)

	return s
}

func (p *cliProgress) StartShared() {
	*p = cliProgress{
		uploading:       1,
		uploadStartTime: time.Now(),
		shared:          true,
	}
}

func (p *cliProgress) FinishShared() {
	atomic.StoreInt32(&p.uploadFinished, 1)
	p.output(defaultColor, "")
}

func (p *cliProgress) UploadStarted(previousFileCount int, previousTotalSize int64) {
	if p.shared {
		// do nothing
		return
	}

	*p = cliProgress{
		uploading:         1,
		uploadStartTime:   time.Now(),
		previousFileCount: previousFileCount,
		previousTotalSize: previousTotalSize,
	}
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
}

var progress = &cliProgress{}

var _ snapshotfs.UploadProgress = (*cliProgress)(nil)
