package storage

import (
	"context"
	"errors"
	"fmt"
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
	ListBlocks(ctx context.Context, prefix string, cb func(bm BlockMetadata) error) error

	// ConnectionInfo returns JSON-serializable data structure containing information required to
	// connect to storage.
	ConnectionInfo() ConnectionInfo

	// Close releases all resources associated with storage.
	Close(ctx context.Context) error
}

// BlockMetadata represents metadata about a single block in a storage.
type BlockMetadata struct {
	BlockID   string
	Length    int64
	Timestamp time.Time
}

// ErrBlockNotFound is returned when a block cannot be found in storage.
var ErrBlockNotFound = errors.New("block not found")

// ListAllBlocks returns BlockMetadata for all blocks in a given storage that have the provided name prefix.
func ListAllBlocks(ctx context.Context, st Storage, prefix string) ([]BlockMetadata, error) {
	var result []BlockMetadata

	err := st.ListBlocks(ctx, prefix, func(bm BlockMetadata) error {
		result = append(result, bm)
		return nil
	})

	return result, err
}

// ListAllBlocksConsistent lists all blocks with given name prefix in the provided storage until the results are
// consistent. The results are consistent if the list result fetched twice is identical. This guarantees that while
// the first scan was in progress, no new block was added or removed.
// maxAttempts specifies maximum number of list attempts (must be >= 2)
func ListAllBlocksConsistent(ctx context.Context, st Storage, prefix string, maxAttempts int) ([]BlockMetadata, error) {
	var previous []BlockMetadata

	for i := 0; i < maxAttempts; i++ {
		result, err := ListAllBlocks(ctx, st, prefix)
		if err != nil {
			return nil, err
		}
		if i > 0 && sameBlocks(result, previous) {
			return result, nil
		}

		previous = result
	}

	return nil, fmt.Errorf("unable to achieve consistent snapshot despite %v attempts", maxAttempts)
}

// sameBlocks returns true if b1 & b2 contain the same blocks (ignoring order).
func sameBlocks(b1, b2 []BlockMetadata) bool {
	if len(b1) != len(b2) {
		return false
	}
	m := map[string]BlockMetadata{}
	for _, b := range b1 {
		m[b.BlockID] = b
	}
	for _, b := range b2 {
		if m[b.BlockID] != b {
			return false
		}
	}
	return true
}
