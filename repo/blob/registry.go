package blob

import (
	"context"

	"github.com/pkg/errors"
)

//nolint:gochecknoglobals
var factories = map[string]*storageFactory{}

// StorageFactory allows creation of repositories in a generic way.
type storageFactory struct {
	defaultConfigFunc func() interface{}
	createStorageFunc func(ctx context.Context, options interface{}, isCreate bool) (Storage, error)
}

// AddSupportedStorage registers factory function to create storage with a given type name.
func AddSupportedStorage[T any](
	urlScheme string,
	defaultConfig T,
	createStorageFunc func(ctx context.Context, options *T, isCreate bool) (Storage, error),
) {
	f := &storageFactory{
		defaultConfigFunc: func() interface{} {
			c := defaultConfig
			return &c
		},
		createStorageFunc: func(ctx context.Context, options interface{}, isCreate bool) (Storage, error) {
			//nolint:forcetypeassert
			return createStorageFunc(ctx, options.(*T), isCreate)
		},
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
