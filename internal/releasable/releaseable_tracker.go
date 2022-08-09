// Package releasable allows process-wide tracking of objects that need to be released.
package releasable

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/pkg/errors"
)

// ItemKind identifies the kind of a releasable item, e.g. "connection", "cache", etc.
type ItemKind string

// Created should be called whenever an item is created. If tracking is enabled, it captures the stack trace of
// the current goroutine and stores it in a map.
func Created(kind ItemKind, itemID interface{}) {
	getPerKind(kind).created(itemID)
}

// Released should be called whenever an item is released.
func Released(kind ItemKind, itemID interface{}) {
	getPerKind(kind).released(itemID)
}

// Active returns the map of all active items.
func Active() map[ItemKind]map[interface{}]string {
	perKindMutex.Lock()
	defer perKindMutex.Unlock()

	res := map[ItemKind]map[interface{}]string{}
	for k, v := range perKindTrackers {
		res[k] = v.active()
	}

	return res
}

// Verify returns error if not all releasable resources have been released.
func Verify() error {
	var buf bytes.Buffer

	for itemKind, active := range Active() {
		if len(active) > 0 {
			fmt.Fprintf(&buf, "found %v %q resources that have not been released:\n", len(active), itemKind)

			for _, stack := range active {
				fmt.Fprintf(&buf, "  - %v\n", stack)
			}
		}
	}

	if buf.Len() == 0 {
		return nil
	}

	return errors.New(buf.String())
}

type perKindTracker struct {
	mu sync.Mutex

	// +checklocks:mu
	items map[interface{}]string
}

func (s *perKindTracker) created(itemID interface{}) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[itemID] = string(debug.Stack())
}

func (s *perKindTracker) released(itemID interface{}) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.items, itemID)
}

func (s *perKindTracker) active() map[interface{}]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := map[interface{}]string{}
	for k, v := range s.items {
		res[k] = v
	}

	return res
}

var (
	perKindMutex sync.Mutex //nolint:gochecknoglobals

	// +checklocks:perKindMutex
	perKindTrackers = map[ItemKind]*perKindTracker{} //nolint:gochecknoglobals
)

// EnableTracking enables tracking of the given item type.
func EnableTracking(kind ItemKind) {
	perKindMutex.Lock()
	defer perKindMutex.Unlock()

	if perKindTrackers[kind] != nil {
		return
	}

	perKindTrackers[kind] = &perKindTracker{
		items: map[interface{}]string{},
	}
}

// DisableTracking disables tracking of the given item type.
func DisableTracking(kind ItemKind) {
	perKindMutex.Lock()
	defer perKindMutex.Unlock()

	delete(perKindTrackers, kind)
}

func getPerKind(kind ItemKind) *perKindTracker {
	perKindMutex.Lock()
	defer perKindMutex.Unlock()

	return perKindTrackers[kind]
}
