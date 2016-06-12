package blob

import (
	"bytes"
	"fmt"
	"io"
	"time"
)

// PutOptions modify the behavior of Storage.PutBlock().
type PutOptions int

// Possible values of PutOptions
const (
	PutOptionsDefault   PutOptions = 0
	PutOptionsOverwrite PutOptions = 1
)

// Storage encapsulates API for connecting to blob storage
type Storage interface {
	PutBlock(id string, data ReaderWithLength, options PutOptions) error
	DeleteBlock(id string) error
	BlockExists(id string) (bool, error)
	GetBlock(id string) ([]byte, error)
	ListBlocks(prefix string) chan (BlockMetadata)
}

// Flusher waits until all pending writes have completed.
type Flusher interface {
	Flush() error
}

// ConnectionInfoProvider exposes persistent ConnectionInfo for connecting to the Storage.
type ConnectionInfoProvider interface {
	ConnectionInfo() ConnectionInfo
}

// ReaderWithLength supports reading from a block and returns its length.
type ReaderWithLength interface {
	io.ReadCloser
	Len() int
}

type bytesReaderWithLength struct {
	*bytes.Buffer
}

// NewReader wraps the provided buffer and returns a ReaderWithLength.
func NewReader(b *bytes.Buffer) ReaderWithLength {
	return &bytesReaderWithLength{b}
}

func (bbr *bytesReaderWithLength) Close() error {
	return nil
}

func (bbr *bytesReaderWithLength) String() string {
	return fmt.Sprintf("buffer(len=%v)", bbr.Len())
}

// BlockMetadata represents metadata about a single block in a blob.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   string
	Length    uint64
	TimeStamp time.Time
	Error     error
}
