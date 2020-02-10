package snapshotfs

// UploadProgress is invoked by by uploader to report status of file and directory uploads.
type UploadProgress interface {
	// UploadStarted is emitted once at the start of an upload
	UploadStarted()

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
}

// NullUploadProgress is an implementation of UploadProgress that does not produce any output.
type NullUploadProgress struct {
}

// UploadStarted implements UploadProgress
func (p *NullUploadProgress) UploadStarted() {}

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

var _ UploadProgress = (*NullUploadProgress)(nil)
