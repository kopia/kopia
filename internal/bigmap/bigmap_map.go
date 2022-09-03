package bigmap

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

const (
	aesNonceSize = 12
	aesKeySize   = 32 // AES-256
)

// Map is a wrapper around internalMap that adds encryption.
type Map struct {
	aead      cipher.AEAD
	inner     *internalMap
	nextNonce *uint64
}

// PutIfAbsent adds the element to the map, returns true if the element was added (as opposed to having
// existed before).
func (s *Map) PutIfAbsent(ctx context.Context, key, value []byte) bool {
	if len(value) == 0 {
		return s.inner.PutIfAbsent(ctx, key, nil)
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	buf := tmp.MakeContiguous(aesNonceSize + len(value) + s.aead.Overhead())
	nonce, input := buf[0:aesNonceSize], buf[aesNonceSize:aesNonceSize+len(value)]
	copy(input, value)

	nonceVal := atomic.AddUint64(s.nextNonce, 1)
	if nonceVal == 0 {
		panic("nonce counter wrapped beyond 64 bits, that should never happen")
	}

	binary.BigEndian.PutUint64(nonce, nonceVal)
	s.aead.Seal(input[:0], nonce, input, key)

	return s.inner.PutIfAbsent(ctx, key, buf)
}

// Get gets the element from the map and appends the value to the provided buffer.
func (s *Map) Get(ctx context.Context, output, key []byte) (result []byte, ok bool, err error) {
	if v, ok := s.inner.Get(output, key); ok {
		result, err := s.decrypt(key, v)

		return result, true, err
	}

	return nil, false, nil
}

func (s *Map) decrypt(key, buf []byte) ([]byte, error) {
	if len(buf) == 0 {
		return nil, nil
	}

	nonce, input := buf[0:aesNonceSize], buf[aesNonceSize:]

	result, err := s.aead.Open(input[:0], nonce, input, key)
	if err != nil {
		return nil, errors.Errorf("unable to decrypt content: %v", err)
	}

	return result, nil
}

// Contains returns true if a given key exists in the set.
func (s *Map) Contains(key []byte) bool {
	return s.inner.Contains(key)
}

// Close releases resources associated with the set.
func (s *Map) Close(ctx context.Context) {
	s.inner.Close(ctx)
}

// NewMap creates new Map.
func NewMap(ctx context.Context) (*Map, error) {
	return NewMapWithOptions(ctx, nil)
}

// NewMapWithOptions creates new Map with options.
func NewMapWithOptions(ctx context.Context, opt *Options) (*Map, error) {
	inner, err := newInternalMapWithOptions(ctx, true, opt)
	if err != nil {
		return nil, err
	}

	key := make([]byte, aesKeySize)

	if _, err = rand.Read(key); err != nil {
		return nil, errors.Wrap(err, "error initializing map key")
	}

	enc, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing map cipher")
	}

	aead, err := cipher.NewGCM(enc)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing map AEAD")
	}

	if aead.NonceSize() != aesNonceSize {
		return nil, errors.Errorf("unexpected nonce size: %v, expected %v", aead.NonceSize(), aesNonceSize)
	}

	return &Map{
		aead:      aead,
		inner:     inner,
		nextNonce: new(uint64),
	}, nil
}
