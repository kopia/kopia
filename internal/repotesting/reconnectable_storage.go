package repotesting

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// reconnectableStorage implements wraps a blob.Storage provider which allows it
// to be reconnected later even if the underlying provider does not have ConnectionInfo().
// the wrapper is cached in a global map and ConnectionInfo() exposes UUID which is the
// map key.
type reconnectableStorage struct {
	blob.Storage

	opt *reconnectableStorageOptions
}

const reconnectableStorageType = "reconnectable"

// reconnectableStorageOptions provides options to reconnectable storage.
type reconnectableStorageOptions struct {
	UUID string
}

// newReconnectableStorage wraps the provided storage that may or may not be round-trippable
// in a wrapper that globally caches storage instance and ensures its connection info is
// round-trippable.
func newReconnectableStorage(tb testing.TB, st blob.Storage) blob.Storage {
	tb.Helper()

	st2 := reconnectableStorage{st, &reconnectableStorageOptions{UUID: uuid.NewString()}}

	reconnectableStorageByUUID.Store(st2.opt.UUID, st2)
	tb.Cleanup(func() {
		reconnectableStorageByUUID.Delete(st2.opt.UUID)
	})

	return st2
}

var reconnectableStorageByUUID sync.Map

func (s reconnectableStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   reconnectableStorageType,
		Config: s.opt,
	}
}

func init() {
	blob.AddSupportedStorage(
		reconnectableStorageType,
		func() interface{} { return &reconnectableStorageOptions{} },
		func(ctx context.Context, o interface{}, isCreate bool) (blob.Storage, error) {
			opt, ok := o.(*reconnectableStorageOptions)
			if !ok {
				return nil, errors.Errorf("invalid options %T", o)
			}

			if opt.UUID == "" {
				return nil, errors.Errorf("missing UUID")
			}

			v, ok := reconnectableStorageByUUID.Load(opt.UUID)
			if !ok {
				return nil, errors.Errorf("reconnectable storage not found: %v", opt.UUID)
			}

			return v.(blob.Storage), nil
		})
}
