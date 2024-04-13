package repomodel

import (
	"context"
	"math/rand"
	"slices"
	"sync"
)

// TrackingSet represents a set of items with built-in.
type TrackingSet[T comparable] struct {
	mu sync.Mutex

	ids []T // +checklocksignore

	setID string // +checklocksignore
}

// PickRandom picks one random manifest from the set or empty string.
func (s *TrackingSet[T]) PickRandom(ctx context.Context) T {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ids) == 0 {
		var defT T

		return defT
	}

	//nolint:gosec
	picked := s.ids[rand.Intn(len(s.ids))]

	log(ctx).Debugw("picked random", "setID", s.setID, "picked", picked)

	return picked
}

// Snapshot returns the snapshot of all IDs.
func (s *TrackingSet[T]) Snapshot(name string) *TrackingSet[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &TrackingSet[T]{
		ids:   append([]T(nil), s.ids...),
		setID: name,
	}
}

// Replace replaces all elements in the set.
func (s *TrackingSet[T]) Replace(ctx context.Context, ids []T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log(ctx).Debugw("replacing set", "setID", s.setID, "ids", ids)
	s.ids = append([]T(nil), ids...)
}

// Add adds the provided items to the set.
func (s *TrackingSet[T]) Add(ctx context.Context, d ...T) {
	if len(d) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log(ctx).Debugw("adding to set", "setID", s.setID, "ids", d)
	s.ids = append(s.ids, d...)
}

// RemoveAll removes the provided items from the set.
func (s *TrackingSet[T]) RemoveAll(ctx context.Context, d ...T) {
	if len(d) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log(ctx).Debugw("removing from set", "setID", s.setID, "ids", d)
	s.ids = removeAll(s.ids, d)
}

func removeAll[T comparable](original, toRemove []T) []T {
	var result []T

	for _, v := range original {
		if !slices.Contains(toRemove, v) {
			result = append(result, v)
		}
	}

	return result
}

// Clear removes all elements from the set.
func (s *TrackingSet[T]) Clear(ctx context.Context) TrackingSet[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.ids
	s.ids = nil

	log(ctx).Debugw("clearing set", "setID", s.setID, "was", old)

	return TrackingSet[T]{ids: old}
}

// NewChangeSet creates new tracking set.
func NewChangeSet[T comparable](setID string) *TrackingSet[T] {
	return &TrackingSet[T]{setID: setID}
}
