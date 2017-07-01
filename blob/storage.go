package blob

import (
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

// CancelFunc requests cancellation of a storage operation.
type CancelFunc func()

// Storage encapsulates API for connecting to blob storage
type Storage interface {
	io.Closer

	BlockSize(id string) (int64, error)
	PutBlock(id string, data []byte, options PutOptions) error
	DeleteBlock(id string) error
	GetBlock(id string) ([]byte, error)
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

// Throttler is an interface optionally implemented by Storage that sets the upload throttle.
type Throttler interface {
	SetThrottle(downloadBytesPerSecond, uploadBytesPerSecond int) error
}
