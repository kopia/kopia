package snapshotfs

import (
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/internal/uitask"
)

const (
	// EstimationTypeClassic represents old way of estimation, which assumes iterating over all files.
	EstimationTypeClassic = "classic"
	// EstimationTypeRough represents new way of estimation, which looks into filesystem stats to get amount of data.
	EstimationTypeRough = "rough"
	// EstimationTypeAdaptive is a combination of new and old approaches. If the estimated file count is high,
	// it will use a rough estimation. If the count is low, it will switch to the classic method.
	EstimationTypeAdaptive = "adaptive"

	// AdaptiveEstimationThreshold is the point at which the classic estimation is used instead of the rough estimation.
	AdaptiveEstimationThreshold = 300000
)

// EstimationParameters represents parameters to be used for estimation.
type EstimationParameters struct {
	Type              string
	AdaptiveThreshold int64
}

// UploadProgress is invoked by uploader to report status of file and directory uploads.
//
//nolint:interfacebloat
type UploadProgress interface {
	// Enabled returns true when progress is enabled, false otherwise.
	Enabled() bool

	// UploadStarted is emitted once at the start of an upload
	UploadStarted()

	// UploadFinished is emitted once at the end of an upload
	UploadFinished()

	// CachedFile is emitted whenever uploader reuses previously uploaded entry without hashing the file.
	CachedFile(path string, size int64)

	// HashingFile is emitted at the beginning of hashing of a given file.
	HashingFile(fname string)

	// ExcludedFile is emitted when a file is excluded.
	ExcludedFile(fname string, size int64)

	// ExcludedDir is emitted when a directory is excluded.
	ExcludedDir(dirname string)

	// FinishedHashingFile is emitted at the end of hashing of a given file.
	FinishedHashingFile(fname string, numBytes int64)

	// FinishedFile is emitted when the uploader is done with a file, regardless of if it was hashed
	// or cached. If an error was encountered it reports that too. A call to FinishedFile gives no
	// information about the reachability of the file in checkpoints that may occur close to the
	// time this function is called.
	FinishedFile(fname string, err error)

	// HashedBytes is emitted while hashing any blocks of bytes.
	HashedBytes(numBytes int64)

	// Error is emitted when an error is encountered.
	Error(path string, err error, isIgnored bool)

	// UploadedBytes is emitted whenever bytes are written to the blob storage.
	UploadedBytes(numBytes int64)

	// StartedDirectory is emitted whenever a directory starts being uploaded.
	StartedDirectory(dirname string)

	// FinishedDirectory is emitted whenever a directory is finished uploading.
	FinishedDirectory(dirname string)

	// EstimationParameters returns settings to be used for estimation
	EstimationParameters() EstimationParameters

	// EstimatedDataSize is emitted whenever the size of upload is estimated.
	EstimatedDataSize(fileCount int64, totalBytes int64)
}

// NullUploadProgress is an implementation of UploadProgress that does not produce any output.
type NullUploadProgress struct{}

// Enabled implements UploadProgress, always returns false.
func (p *NullUploadProgress) Enabled() bool {
	return false
}

// UploadStarted implements UploadProgress.
func (p *NullUploadProgress) UploadStarted() {}

// EstimatedDataSize implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) EstimatedDataSize(fileCount, totalBytes int64) {}

// UploadFinished implements UploadProgress.
func (p *NullUploadProgress) UploadFinished() {}

// HashedBytes implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) HashedBytes(numBytes int64) {}

// ExcludedFile implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) ExcludedFile(fname string, numBytes int64) {}

// ExcludedDir implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) ExcludedDir(dirname string) {}

// CachedFile implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) CachedFile(fname string, numBytes int64) {}

// UploadedBytes implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) UploadedBytes(numBytes int64) {}

// HashingFile implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) HashingFile(fname string) {}

// FinishedHashingFile implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) FinishedHashingFile(fname string, numBytes int64) {}

// FinishedFile implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) FinishedFile(fname string, err error) {}

// StartedDirectory implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) StartedDirectory(dirname string) {}

// FinishedDirectory implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) FinishedDirectory(dirname string) {}

// Error implements UploadProgress.
//
//nolint:revive
func (p *NullUploadProgress) Error(path string, err error, isIgnored bool) {}

// EstimationParameters implements UploadProgress.
func (p *NullUploadProgress) EstimationParameters() EstimationParameters {
	return EstimationParameters{
		Type: EstimationTypeClassic,
	}
}

var _ UploadProgress = (*NullUploadProgress)(nil)

// UploadCounters represents a snapshot of upload counters.
type UploadCounters struct {
	// +checkatomic
	TotalCachedBytes int64 `json:"cachedBytes"`
	// +checkatomic
	TotalHashedBytes int64 `json:"hashedBytes"`
	// +checkatomic
	TotalUploadedBytes int64 `json:"uploadedBytes"`

	// +checkatomic
	EstimatedBytes int64 `json:"estimatedBytes"`

	// +checkatomic
	TotalCachedFiles int32 `json:"cachedFiles"`
	// +checkatomic
	TotalHashedFiles int32 `json:"hashedFiles"`

	// +checkatomic
	TotalExcludedFiles int32 `json:"excludedFiles"`
	// +checkatomic
	TotalExcludedDirs int32 `json:"excludedDirs"`

	// +checkatomic
	FatalErrorCount int32 `json:"errors"`
	// +checkatomic
	IgnoredErrorCount int32 `json:"ignoredErrors"`
	// +checkatomic
	EstimatedFiles int64 `json:"estimatedFiles"`

	CurrentDirectory string `json:"directory"`

	LastErrorPath string `json:"lastErrorPath"`
	LastError     string `json:"lastError"`
}

// CountingUploadProgress is an implementation of UploadProgress that accumulates counters.
type CountingUploadProgress struct {
	NullUploadProgress

	mu sync.Mutex

	counters UploadCounters
}

// UploadStarted implements UploadProgress.
func (p *CountingUploadProgress) UploadStarted() {
	// reset counters to all-zero values.
	p.counters = UploadCounters{}
}

// UploadedBytes implements UploadProgress.
func (p *CountingUploadProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.counters.TotalUploadedBytes, numBytes)
}

// EstimatedDataSize implements UploadProgress.
func (p *CountingUploadProgress) EstimatedDataSize(numFiles, numBytes int64) {
	atomic.StoreInt64(&p.counters.EstimatedBytes, numBytes)
	atomic.StoreInt64(&p.counters.EstimatedFiles, numFiles)
}

// HashedBytes implements UploadProgress.
func (p *CountingUploadProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.counters.TotalHashedBytes, numBytes)
}

// CachedFile implements UploadProgress.
//
//nolint:revive
func (p *CountingUploadProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalCachedFiles, 1)
	atomic.AddInt64(&p.counters.TotalCachedBytes, numBytes)
}

// FinishedHashingFile implements UploadProgress.
//
//nolint:revive
func (p *CountingUploadProgress) FinishedHashingFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalHashedFiles, 1)
}

// FinishedFile implements UploadProgress.
//
//nolint:revive
func (p *CountingUploadProgress) FinishedFile(fname string, err error) {}

// ExcludedDir implements UploadProgress.
//
//nolint:revive
func (p *CountingUploadProgress) ExcludedDir(dirname string) {
	atomic.AddInt32(&p.counters.TotalExcludedDirs, 1)
}

// ExcludedFile implements UploadProgress.
//
//nolint:revive
func (p *CountingUploadProgress) ExcludedFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalExcludedFiles, 1)
}

// Error implements UploadProgress.
func (p *CountingUploadProgress) Error(path string, err error, isIgnored bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if isIgnored {
		atomic.AddInt32(&p.counters.IgnoredErrorCount, 1)
	} else {
		atomic.AddInt32(&p.counters.FatalErrorCount, 1)
	}

	p.counters.LastErrorPath = path
	p.counters.LastError = err.Error()
}

// StartedDirectory implements UploadProgress.
func (p *CountingUploadProgress) StartedDirectory(dirname string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.counters.CurrentDirectory = dirname
}

// Snapshot captures current snapshot of the upload.
func (p *CountingUploadProgress) Snapshot() UploadCounters {
	p.mu.Lock()
	defer p.mu.Unlock()

	return UploadCounters{
		TotalCachedFiles:  atomic.LoadInt32(&p.counters.TotalCachedFiles),
		TotalHashedFiles:  atomic.LoadInt32(&p.counters.TotalHashedFiles),
		TotalCachedBytes:  atomic.LoadInt64(&p.counters.TotalCachedBytes),
		TotalHashedBytes:  atomic.LoadInt64(&p.counters.TotalHashedBytes),
		EstimatedBytes:    atomic.LoadInt64(&p.counters.EstimatedBytes),
		EstimatedFiles:    atomic.LoadInt64(&p.counters.EstimatedFiles),
		IgnoredErrorCount: atomic.LoadInt32(&p.counters.IgnoredErrorCount),
		FatalErrorCount:   atomic.LoadInt32(&p.counters.FatalErrorCount),
		CurrentDirectory:  p.counters.CurrentDirectory,
		LastErrorPath:     p.counters.LastErrorPath,
		LastError:         p.counters.LastError,
	}
}

// UITaskCounters returns UI task counters.
func (p *CountingUploadProgress) UITaskCounters(final bool) map[string]uitask.CounterValue {
	cachedFiles := int64(atomic.LoadInt32(&p.counters.TotalCachedFiles))
	hashedFiles := int64(atomic.LoadInt32(&p.counters.TotalHashedFiles))

	cachedBytes := atomic.LoadInt64(&p.counters.TotalCachedBytes)
	hashedBytes := atomic.LoadInt64(&p.counters.TotalHashedBytes)

	m := map[string]uitask.CounterValue{
		"Cached Files":    uitask.SimpleCounter(cachedFiles),
		"Hashed Files":    uitask.SimpleCounter(hashedFiles),
		"Processed Files": uitask.SimpleCounter(hashedFiles + cachedFiles),

		"Cached Bytes":    uitask.BytesCounter(cachedBytes),
		"Hashed Bytes":    uitask.BytesCounter(hashedBytes),
		"Processed Bytes": uitask.BytesCounter(hashedBytes + cachedBytes),

		// bytes actually ploaded to the server (non-deduplicated)
		"Uploaded Bytes": uitask.BytesCounter(atomic.LoadInt64(&p.counters.TotalUploadedBytes)),

		"Excluded Files":       uitask.SimpleCounter(int64(atomic.LoadInt32(&p.counters.TotalExcludedFiles))),
		"Excluded Directories": uitask.SimpleCounter(int64(atomic.LoadInt32(&p.counters.TotalExcludedDirs))),

		"Errors": uitask.ErrorCounter(int64(atomic.LoadInt32(&p.counters.FatalErrorCount))),
	}

	if !final {
		m["Estimated Files"] = uitask.SimpleCounter(atomic.LoadInt64(&p.counters.EstimatedFiles))
		m["Estimated Bytes"] = uitask.BytesCounter(atomic.LoadInt64(&p.counters.EstimatedBytes))
	}

	return m
}

var _ UploadProgress = (*CountingUploadProgress)(nil)
