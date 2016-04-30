package blob

import (
	"io"
	"net/url"
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
	PutBlock(id string, data io.ReadCloser, options PutOptions) error
	DeleteBlock(id string) error
	Flush() error
	BlockExists(id string) (bool, error)
	GetBlock(id string) ([]byte, error)
	ListBlocks(prefix string) chan (BlockMetadata)
	Configuration() StorageConfiguration
}

// BlockMetadata represents metadata about a single block in a blob.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   string
	Length    uint64
	TimeStamp time.Time
	Error     error
}

type StorageOptions interface {
	ParseURL(u *url.URL) error
	ToURL() *url.URL
}
