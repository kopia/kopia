package snapshotfs

import (
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/internal/uitask"
)

// UploadProgress is invoked by uploader to report status of file and directory uploads.
//
//nolint:interfacebloat
type UploadProgress interface {
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

	// EstimatedDataSize is emitted whenever the size of upload is estimated.
	EstimatedDataSize(fileCount int, totalBytes int64)
}

// NullUploadProgress is an implementation of UploadProgress that does not produce any output.
type NullUploadProgress struct{}

// UploadStarted implements UploadProgress.
func (p *NullUploadProgress) UploadStarted() {}

// EstimatedDataSize implements UploadProgress.
func (p *NullUploadProgress) EstimatedDataSize(fileCount int, totalBytes int64) {}

// UploadFinished implements UploadProgress.
func (p *NullUploadProgress) UploadFinished() {}

// HashedBytes implements UploadProgress.
func (p *NullUploadProgress) HashedBytes(numBytes int64) {}

// ExcludedFile implements UploadProgress.
func (p *NullUploadProgress) ExcludedFile(fname string, numBytes int64) {}

// ExcludedDir implements UploadProgress.
func (p *NullUploadProgress) ExcludedDir(dirname string) {}

// CachedFile implements UploadProgress.
func (p *NullUploadProgress) CachedFile(fname string, numBytes int64) {}

// UploadedBytes implements UploadProgress.
func (p *NullUploadProgress) UploadedBytes(numBytes int64) {}

// HashingFile implements UploadProgress.
func (p *NullUploadProgress) HashingFile(fname string) {}

// FinishedHashingFile implements UploadProgress.
func (p *NullUploadProgress) FinishedHashingFile(fname string, numBytes int64) {}

// FinishedFile implements UploadProgress.
func (p *NullUploadProgress) FinishedFile(fname string, err error) {}

// StartedDirectory implements UploadProgress.
func (p *NullUploadProgress) StartedDirectory(dirname string) {}

// FinishedDirectory implements UploadProgress.
func (p *NullUploadProgress) FinishedDirectory(dirname string) {}

// Error implements UploadProgress.
func (p *NullUploadProgress) Error(path string, err error, isIgnored bool) {}

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
	EstimatedFiles int32 `json:"estimatedFiles"`

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
func (p *CountingUploadProgress) EstimatedDataSize(numFiles int, numBytes int64) {
	atomic.StoreInt64(&p.counters.EstimatedBytes, numBytes)
	atomic.StoreInt32(&p.counters.EstimatedFiles, int32(numFiles))
}

// HashedBytes implements UploadProgress.
func (p *CountingUploadProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.counters.TotalHashedBytes, numBytes)
}

// CachedFile implements UploadProgress.
func (p *CountingUploadProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalCachedFiles, 1)
	atomic.AddInt64(&p.counters.TotalCachedBytes, numBytes)
}

// FinishedHashingFile implements UploadProgress.
func (p *CountingUploadProgress) FinishedHashingFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalHashedFiles, 1)
}

// FinishedFile implements UploadProgress.
func (p *CountingUploadProgress) FinishedFile(fname string, err error) {}

// ExcludedDir implements UploadProgress.
func (p *CountingUploadProgress) ExcludedDir(dirname string) {
	atomic.AddInt32(&p.counters.TotalExcludedDirs, 1)
}

// ExcludedFile implements UploadProgress.
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
		TotalCachedFiles: atomic.LoadInt32(&p.counters.TotalCachedFiles),
		TotalHashedFiles: atomic.LoadInt32(&p.counters.TotalHashedFiles),
		TotalCachedBytes: atomic.LoadInt64(&p.counters.TotalCachedBytes),
		TotalHashedBytes: atomic.LoadInt64(&p.counters.TotalHashedBytes),
		EstimatedBytes:   atomic.LoadInt64(&p.counters.EstimatedBytes),
		EstimatedFiles:   atomic.LoadInt32(&p.counters.EstimatedFiles),
		CurrentDirectory: p.counters.CurrentDirectory,
		LastErrorPath:    p.counters.LastErrorPath,
		LastError:        p.counters.LastError,
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

		"Errors": uitask.ErrorCounter(int64(atomic.LoadInt32(&p.counters.IgnoredErrorCount))),
	}

	if !final {
		m["Estimated Files"] = uitask.SimpleCounter(int64(atomic.LoadInt32(&p.counters.EstimatedFiles)))
		m["Estimated Bytes"] = uitask.BytesCounter(atomic.LoadInt64(&p.counters.EstimatedBytes))
	}

	return m
}

var _ UploadProgress = (*CountingUploadProgress)(nil)
