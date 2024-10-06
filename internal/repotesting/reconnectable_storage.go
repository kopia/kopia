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

	opt *ReconnectableStorageOptions
}

// ReconnectableStorageType is the unique storage type identifier for
// reconnectable storage backend.
const ReconnectableStorageType = "reconnectable"

// ReconnectableStorageOptions provides options to reconnectable storage.
type ReconnectableStorageOptions struct {
	UUID string
}

// NewReconnectableStorage wraps the provided storage that may or may not be round-trippable
// in a wrapper that globally caches storage instance and ensures its connection info is
// round-trippable.
func NewReconnectableStorage(tb testing.TB, st blob.Storage) blob.Storage {
	tb.Helper()

	st2 := reconnectableStorage{st, &ReconnectableStorageOptions{UUID: uuid.NewString()}}

	reconnectableStorageByUUID.Store(st2.opt.UUID, st2)
	tb.Cleanup(func() {
		reconnectableStorageByUUID.Delete(st2.opt.UUID)
	})

	return st2
}

//nolint:gochecknoglobals
var reconnectableStorageByUUID sync.Map

func (s reconnectableStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   ReconnectableStorageType,
		Config: s.opt,
	}
}

// New creates new reconnectable storage.
func New(ctx context.Context, opt *ReconnectableStorageOptions, isCreate bool) (blob.Storage, error) {
	if opt.UUID == "" {
		return nil, errors.New("missing UUID")
	}

	v, ok := reconnectableStorageByUUID.Load(opt.UUID)
	if !ok {
		return nil, errors.Errorf("reconnectable storage not found: %v", opt.UUID)
	}

	return v.(blob.Storage), nil
}

func init() {
	blob.AddSupportedStorage(ReconnectableStorageType, ReconnectableStorageOptions{}, New)
}
