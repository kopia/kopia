package storage

import (
	"io"
	"time"
)

// BlockID represents the identifier of a block stored in a Repository.
type BlockID string

// PutOptions controls the behavior of Repository.PutBlock()
type PutOptions struct {
	Overwrite    bool
	IgnoreLimits bool
}

// Repository encapsulates storage for blocks of data.
type RepositoryWriter interface {
	// BlockExists determines whether the specified block existts.
	PutBlock(id BlockID, data io.ReadCloser, options PutOptions) error
	DeleteBlock(id BlockID) error
	Flush() error
}

// Repository encapsulates storage for blocks of data.
type RepositoryReader interface {
	BlockExists(id BlockID) (bool, error)
	GetBlock(id BlockID) ([]byte, error)
	ListBlocks(prefix BlockID) chan (BlockMetadata)
}

type Repository interface {
	RepositoryReader
	RepositoryWriter
	Configuration() RepositoryConfiguration
}

// BlockMetadata represents metadata about a single block in a repository.
// If Error field is set, no other field values should be assumed to be correct.
type BlockMetadata struct {
	BlockID   BlockID
	Length    uint64
	TimeStamp time.Time
	Error     error
}
