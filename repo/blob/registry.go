package blob

import (
	"context"

	"github.com/pkg/errors"
)

// CreateStorageFunc is a function that returns Storage with provided options, optionally
// creating the underlying storage location (e.g. directory), when possible.
type CreateStorageFunc func(ctx context.Context, options interface{}, isCreate bool) (Storage, error)

//nolint:gochecknoglobals
var factories = map[string]*storageFactory{}

// StorageFactory allows creation of repositories in a generic way.
type storageFactory struct {
	defaultConfigFunc func() interface{}
	createStorageFunc CreateStorageFunc
}

// AddSupportedStorage registers factory function to create storage with a given type name.
func AddSupportedStorage(
	urlScheme string,
	defaultConfigFunc func() interface{},
	createStorageFunc CreateStorageFunc,
) {
	f := &storageFactory{
		defaultConfigFunc: defaultConfigFunc,
		createStorageFunc: createStorageFunc,
	}

	factories[urlScheme] = f
}

// NewStorage creates new storage based on ConnectionInfo.
// The storage type must be previously registered using AddSupportedStorage.
func NewStorage(ctx context.Context, cfg ConnectionInfo, isCreate bool) (Storage, error) {
	if factory, ok := factories[cfg.Type]; ok {
		return factory.createStorageFunc(ctx, cfg.Config, isCreate)
	}

	return nil, errors.Errorf("unknown storage type: %s", cfg.Type)
}
