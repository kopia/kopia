package blob

import "fmt"

var (
	factories = map[string]storageFactory{}
)

// StorageFactory allows creation of repositories in a generic way.
type storageFactory struct {
	defaultConfigFunc func() interface{}
	createStorageFunc func(interface{}) (Storage, error)
}

// AddSupportedStorage registers factory function to create storage with a given type name.
func AddSupportedStorage(
	storageType string,
	defaultConfigFunc func() interface{},
	createStorageFunc func(interface{}) (Storage, error)) {

	factories[storageType] = storageFactory{
		defaultConfigFunc: defaultConfigFunc,
		createStorageFunc: createStorageFunc,
	}
}

// NewStorage creates new storage based on StorageConfiguration.
// The storage type must be previously registered using AddSupportedStorage.
func NewStorage(cfg StorageConfiguration) (Storage, error) {
	if factory, ok := factories[cfg.Type]; ok {
		return factory.createStorageFunc(cfg.Config)
	}

	return nil, fmt.Errorf("unknown storage type: %s", cfg.Type)
}
