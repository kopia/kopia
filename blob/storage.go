package blob

import (
	"bytes"
	"io"
	"time"
)

// PutOptions controls the behavior of Storage.PutBlock()
type PutOptions struct {
	Overwrite    bool
	IgnoreLimits bool
}

// Storage encapsulates API for connecting to blob storage
type Storage interface {
	// BlockExists determines whether the specified block existts.
	PutBlock(id string, data BlockReader, options PutOptions) error
	DeleteBlock(id string) error
	Flush() error
	BlockExists(id string) (bool, error)
	GetBlock(id string) ([]byte, error)
	ListBlocks(prefix string) chan (BlockMetadata)
	Configuration() StorageConfiguration
}

// BlockReader supports reading from a block and returns its length.
type BlockReader interface {
	io.ReadCloser
	Len() int
}

type bytesBlockReader struct {
	*bytes.Buffer
}

// NewBlockReader wraps the provided buffer and returns a BlockReader.
func NewBlockReader(b *bytes.Buffer) BlockReader {
	return &bytesBlockReader{b}
}

func (bbr *bytesBlockReader) Close() error {
	return nil
}

// BlockMetadata represents metadata about a single block in a blob.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   string
	Length    uint64
	TimeStamp time.Time
	Error     error
}
