//go:build darwin || (linux && amd64)

package robustness

import "context"

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
type Store interface {
	Store(ctx context.Context, key string, val []byte) error
	Load(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// Persister describes the ability to flush metadata
// to, and load it again, from a repository.
type Persister interface {
	Store
	LoadMetadata() error
	FlushMetadata() error
	GetPersistDir() string
}
