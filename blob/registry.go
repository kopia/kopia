package blob

import (
	"fmt"
	"net/url"
)

var (
	factories = map[string]*storageFactory{}
)

// StorageFactory allows creation of repositories in a generic way.
type storageFactory struct {
	defaultConfigFunc func() StorageOptions
	createStorageFunc func(StorageOptions) (Storage, error)
}

// AddSupportedStorage registers factory function to create storage with a given type name.
func AddSupportedStorage(
	urlScheme string,
	defaultConfigFunc func() StorageOptions,
	createStorageFunc func(StorageOptions) (Storage, error)) {

	f := &storageFactory{
		defaultConfigFunc: defaultConfigFunc,
		createStorageFunc: createStorageFunc,
	}
	factories[urlScheme] = f
}

// NewStorage creates new storage based on StorageConfiguration.
// The storage type must be previously registered using AddSupportedStorage.
func NewStorage(cfg StorageConfiguration) (Storage, error) {
	if factory, ok := factories[cfg.Type]; ok {
		return factory.createStorageFunc(cfg.Config)
	}

	return nil, fmt.Errorf("unknown storage type: %s", cfg.Type)
}

// NewStorageFromURL creates new storage based on a URL.
// The storage type must be previously registered using AddSupportedStorage.
func NewStorageFromURL(storageURL string) (Storage, error) {
	u, err := url.Parse(storageURL)
	if err != nil {
		return nil, err
	}
	if factory, ok := factories[u.Scheme]; ok {
		o := factory.defaultConfigFunc()
		if err := o.ParseURL(u); err != nil {
			return nil, err
		}

		return factory.createStorageFunc(o)
	}
	return nil, fmt.Errorf("unknown storage type: %s", u.Scheme)
}
