package storage

import (
	"context"
	"errors"
	"time"
)

// CancelFunc requests cancellation of a storage operation.
type CancelFunc func()

// Storage encapsulates API for connecting to blob storage.
//
// The underlying storage system must provide:
//
// * high durability, availability and bit-rot protection
// * read-after-write - block written using PutBlock() must be immediately readable using GetBlock() and ListBlocks()
// * atomicity - it mustn't be possible to observe partial results of PutBlock() via either GetBlock() or ListBlocks()
// * timestamps that don't go back in time (small clock skew up to minutes is allowed)
// * reasonably low latency for retrievals
//
// The required semantics are provided by existing commercial cloud storage products (Google Cloud, AWS, Azure).
type Storage interface {
	// PutBlock uploads the block with given data to the repository or replaces existing block with the provided
	// id with given contents.
	PutBlock(ctx context.Context, id string, data []byte) error

	// DeleteBlock removes the block from storage. Future GetBlock() operations will fail with ErrBlockNotFound.
	DeleteBlock(ctx context.Context, id string) error

	// GetBlock returns full or partial contents of a block with given ID.
	// If length>0, the the function retrieves a range of bytes [offset,offset+length)
	// If length<0, the entire block must be fetched.
	GetBlock(ctx context.Context, id string, offset, length int64) ([]byte, error)

	// ListBlocks returns a channel of BlockMetadata that describes storage blocks with existing name prefixes.
	// Iteration continues until all blocks have been listed or until client code invokes the returned cancellation function.
	ListBlocks(ctx context.Context, prefix string) <-chan BlockMetadata

	// ConnectionInfo returns JSON-serializable data structure containing information required to
	// connect to storage.
	ConnectionInfo() ConnectionInfo

	// Close releases all resources associated with storage.
	Close(ctx context.Context) error
}

// BlockMetadata represents metadata about a single block in a storage.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   string
	Length    int64
	TimeStamp time.Time
	Error     error
}

// ErrBlockNotFound is returned when a block cannot be found in storage.
var ErrBlockNotFound = errors.New("block not found")
