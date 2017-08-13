package blob

import (
	"io"
	"time"
)

// CancelFunc requests cancellation of a storage operation.
type CancelFunc func()

// Storage encapsulates API for connecting to blob storage
type Storage interface {
	io.Closer

	BlockSize(id string) (int64, error)
	PutBlock(id string, data []byte) error
	DeleteBlock(id string) error
	GetBlock(id string, offset, length int64) ([]byte, error)
	ListBlocks(prefix string) (chan (BlockMetadata), CancelFunc)
}

// ConnectionInfoProvider exposes persistent ConnectionInfo for connecting to the Storage.
type ConnectionInfoProvider interface {
	ConnectionInfo() ConnectionInfo
}

// BlockMetadata represents metadata about a single block in a storage.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   string
	Length    int64
	TimeStamp time.Time
	Error     error
}
