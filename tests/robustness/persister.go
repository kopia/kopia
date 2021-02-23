package robustness

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
type Store interface {
	Store(key string, val []byte) error
	Load(key string) ([]byte, error)
	Delete(key string)
}

// Persister describes the ability to flush metadata
// to, and load it again, from a repository.
type Persister interface {
	Store
	LoadMetadata() error
	FlushMetadata() error
	GetPersistDir() string
}
