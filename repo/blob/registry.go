package blob

import (
	"context"

	"github.com/pkg/errors"
)

var factories = map[string]*storageFactory{}

// StorageFactory allows creation of repositories in a generic way.
type storageFactory struct {
	defaultConfigFunc func() interface{}
	createStorageFunc func(context.Context, interface{}) (Storage, error)
}

// AddSupportedStorage registers factory function to create storage with a given type name.
func AddSupportedStorage(
	urlScheme string,
	defaultConfigFunc func() interface{},
	createStorageFunc func(context.Context, interface{}) (Storage, error),
) {
	f := &storageFactory{
		defaultConfigFunc: defaultConfigFunc,
		createStorageFunc: createStorageFunc,
	}

	factories[urlScheme] = f
}

// NewStorage creates new storage based on ConnectionInfo.
// The storage type must be previously registered using AddSupportedStorage.
func NewStorage(ctx context.Context, cfg ConnectionInfo) (Storage, error) {
	if factory, ok := factories[cfg.Type]; ok {
		return factory.createStorageFunc(ctx, cfg.Config)
	}

	return nil, errors.Errorf("unknown storage type: %s", cfg.Type)
}
