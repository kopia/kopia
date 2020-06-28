// Package snapmeta describes entities that can accept
// arbitrary metadata and flush it to a persistent repository.
package snapmeta

import "github.com/kopia/kopia/tests/robustness/snap"

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
type Store interface {
	Store(key string, val []byte) error
	Load(key string) ([]byte, error)
	Delete(key string)
	GetKeys() []string
}

// Persister describes the ability to flush metadata
// to, and load it again, from a repository.
type Persister interface {
	Store
	snap.RepoManager
	LoadMetadata() error
	FlushMetadata() error
	Cleanup()
}
