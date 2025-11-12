//go:build darwin || (linux && amd64)

package snapmeta

import (
	"context"

	"github.com/kopia/kopia/tests/robustness"
)

var _ robustness.Store = &Simple{}

// Simple is a snapstore implementation that stores
// snapshot metadata as a byte slice in a map in memory.
// A Simple should not be copied.
type Simple struct {
	Data map[string][]byte `json:"data"`
}

// NewSimple instantiates a new Simple snapstore and
// returns its pointer.
func NewSimple() *Simple {
	return &Simple{
		Data: make(map[string][]byte),
	}
}

// Store implements the Storer interface Store method.
func (s *Simple) Store(ctx context.Context, key string, val []byte) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.Data[key] = buf

	return nil
}

// Load implements the Storer interface Load method.
func (s *Simple) Load(ctx context.Context, key string) ([]byte, error) {
	if buf, found := s.Data[key]; found {
		retBuf := make([]byte, len(buf))
		_ = copy(retBuf, buf)

		return retBuf, nil
	}

	return nil, robustness.ErrKeyNotFound
}

// Delete implements the Storer interface Delete method.
func (s *Simple) Delete(ctx context.Context, key string) error {
	delete(s.Data, key)

	return nil
}
