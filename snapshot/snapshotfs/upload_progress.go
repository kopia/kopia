package snapshotfs

import (
	"sync"
	"sync/atomic"
)

// UploadProgress is invoked by by uploader to report status of file and directory uploads.
type UploadProgress interface {
	// UploadStarted is emitted once at the start of an upload
	UploadStarted(previousFileCount int, previousTotalSize int64)

	// UploadFinished is emitted once at the end of an upload
	UploadFinished()

	// CachedFile is emitted whenever uploader reuses previously uploaded entry without hashing the file.
	CachedFile(path string, size int64)

	// HashingFile is emitted at the beginning of hashing of a given file.
	HashingFile(fname string)

	// FinishedHashingFile is emitted at the end of hashing of a given file.
	FinishedHashingFile(fname string, numBytes int64)

	// HashedBytes is emitted while hashing any blocks of bytes.
	HashedBytes(numBytes int64)

	// UploadedBytes is emitted whenever bytes are written to the blob storage.
	UploadedBytes(numBytes int64)

	// StartedDirectory is emitted whenever a directory starts being uploaded.
	StartedDirectory(dirname string)

	// FinishedDirectory is emitted whenever a directory is finished uploading.
	FinishedDirectory(dirname string)
}

// NullUploadProgress is an implementation of UploadProgress that does not produce any output.
type NullUploadProgress struct {
}

// UploadStarted implements UploadProgress
func (p *NullUploadProgress) UploadStarted(previousFileCount int, previousTotalSize int64) {}

// UploadFinished implements UploadProgress
func (p *NullUploadProgress) UploadFinished() {}

// HashedBytes implements UploadProgress
func (p *NullUploadProgress) HashedBytes(numBytes int64) {}

// CachedFile implements UploadProgress
func (p *NullUploadProgress) CachedFile(fname string, numBytes int64) {}

// UploadedBytes implements UploadProgress
func (p *NullUploadProgress) UploadedBytes(numBytes int64) {}

// HashingFile implements UploadProgress
func (p *NullUploadProgress) HashingFile(fname string) {}

// FinishedHashingFile implements UploadProgress
func (p *NullUploadProgress) FinishedHashingFile(fname string, numBytes int64) {}

// StartedDirectory implements UploadProgress
func (p *NullUploadProgress) StartedDirectory(dirname string) {}

// FinishedDirectory implements UploadProgress
func (p *NullUploadProgress) FinishedDirectory(dirname string) {}

var _ UploadProgress = (*NullUploadProgress)(nil)

// UploadCounters represents a snapshot of upload counters.
type UploadCounters struct {
	TotalCachedBytes int64 `json:"cachedBytes"`
	TotalHashedBytes int64 `json:"hashedBytes"`

	TotalCachedFiles int32 `json:"cachedFiles"`
	TotalHashedFiles int32 `json:"hashedFiles"`

	CurrentDirectory string `json:"directory"`
}

// CountingUploadProgress is an implementation of UploadProgress that accumulates counters.
type CountingUploadProgress struct {
	NullUploadProgress

	mu sync.Mutex

	counters UploadCounters
}

// UploadStarted implements UploadProgress
func (p *CountingUploadProgress) UploadStarted(previousFileCount int, previousTotalFileSize int64) {
	// reset counters to all-zero values.
	p.counters = UploadCounters{}
}

// HashedBytes implements UploadProgress
func (p *CountingUploadProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.counters.TotalHashedBytes, numBytes)
}

// CachedFile implements UploadProgress
func (p *CountingUploadProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalCachedFiles, 1)
	atomic.AddInt64(&p.counters.TotalCachedBytes, numBytes)
}

// FinishedHashingFile implements UploadProgress
func (p *CountingUploadProgress) FinishedHashingFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.counters.TotalHashedFiles, 1)
}

// StartedDirectory implements UploadProgress
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
		CurrentDirectory: p.counters.CurrentDirectory,
	}
}

var _ UploadProgress = (*CountingUploadProgress)(nil)
