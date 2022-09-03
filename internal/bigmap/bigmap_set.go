package bigmap

import "context"

// Set is a wrapper around Map that only supports Put() and Contains().
type Set struct {
	inner *internalMap
}

// Put adds the element to a set, returns true if the element was added (as opposed to having
// existed before).
func (s *Set) Put(ctx context.Context, key []byte) bool {
	return s.inner.PutIfAbsent(ctx, key, nil)
}

// Contains returns true if a given key exists in the set.
func (s *Set) Contains(key []byte) bool {
	return s.inner.Contains(key)
}

// Close releases resources associated with the set.
func (s *Set) Close(ctx context.Context) {
	s.inner.Close(ctx)
}

// NewSet creates new Set.
func NewSet(ctx context.Context) (*Set, error) {
	return NewSetWithOptions(ctx, nil)
}

// NewSetWithOptions creates new Set with options.
func NewSetWithOptions(ctx context.Context, opt *Options) (*Set, error) {
	inner, err := newInternalMapWithOptions(ctx, false, opt)
	if err != nil {
		return nil, err
	}

	return &Set{inner}, nil
}
