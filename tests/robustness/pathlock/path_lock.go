// Package pathlock defines a PathLocker interface and an implementation
// that will synchronize based on filepath.
package pathlock

import (
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Locker is an interface for synchronizing on a given filepath.
// A call to Lock a given path will block any asynchronous calls to Lock
// that same path, or any parent or child path in the same sub-tree.
// For example:
//   - Lock path /a/b/c
//   - Blocks a Lock call for the same path /a/b/c
//   - Blocks a Lock call for path /a/b or /a
//   - Blocks a Lock call for path /a/b/c/d
//   - Allows a Lock call for path /a/b/x
//   - Allows a Lock call for path /a/x
type Locker interface {
	Lock(path string) (Unlocker, error)
}

// Unlocker unlocks from a previous invocation of Lock().
type Unlocker interface {
	Unlock()
}

var _ Locker = (*pathLock)(nil)

// pathLock is a path-based mutex mechanism that allows for synchronization
// along subpaths. A call to Lock will block as long as the requested path
// is equal to, or otherwise in the path of (e.g. parent/child) another path
// that has already been Locked. The thread will be blocked until the holder
// of the lock calls Unlock.
type pathLock struct {
	mu sync.Mutex

	// +checklocks:mu
	lockedPaths map[string]chan struct{}
}

// NewLocker returns a Locker.
func NewLocker() Locker {
	return &pathLock{
		lockedPaths: make(map[string]chan struct{}),
	}
}

type lock struct {
	pl   *pathLock
	path string
}

func (l *lock) Unlock() {
	l.pl.unlock(l.path)
}

// busyCounter is for unit testing, to determine whether a Lock has been
// called and blocked.
var busyCounter uint64

// Lock will lock the given path, preventing concurrent calls to Lock
// for that path, or any parent/child path, until Unlock has been called.
// Any concurrent Lock calls will block until that path is available.
func (pl *pathLock) Lock(path string) (Unlocker, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	for {
		ch, err := pl.tryToLockPath(absPath)
		if err != nil {
			return nil, err
		}

		if ch == nil {
			break
		}

		atomic.AddUint64(&busyCounter, 1)

		<-ch
	}

	return &lock{
		pl:   pl,
		path: absPath,
	}, nil
}

// tryToLockPath is a helper for locking a given path/subpath.
// It locks the common mutex while accessing the internal map of locked
// paths. Each element in the list of locked paths is tested for whether
// or not it is within the same subtree as the requested path to lock.
//
// If none of the already-reserved paths coincide with this one, this
// goroutine can safely lock this path. To do so, it creates a
// new map entry whose key is the locked path, and whose value is
// a channel that other goroutines can wait on, should there be
// a collision.
//
// If this goroutine DOES find a conflicting path, that path's
// channel is returned. The caller can wait on that channel. After
// the channel is closed, the caller should try again by calling
// `tryToLockPath` until no channel is returned (indicating the lock
// has been claimed).
func (pl *pathLock) tryToLockPath(path string) (chan struct{}, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	for lockedPath, ch := range pl.lockedPaths {
		var (
			pathInLockedPath, lockedPathInPath bool
			err                                error
		)

		if pathInLockedPath, err = isInPath(path, lockedPath); err == nil {
			lockedPathInPath, err = isInPath(lockedPath, path)
		}

		if err != nil {
			return nil, err
		}

		if pathInLockedPath || lockedPathInPath {
			return ch, nil
		}
	}

	pl.lockedPaths[path] = make(chan struct{})

	return nil, nil
}

// unlock will unlock the given path. It is assumed that Lock
// has already been called, and that unlock will be called once
// and only once with the exact path provided to the Lock function.
func (pl *pathLock) unlock(path string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	close(pl.lockedPaths[path])
	delete(pl.lockedPaths, path)
}

// isInPath is a helper to determine whether one path is
// either the same as another, or a child path (recursively) of it.
func isInPath(path1, path2 string) (bool, error) {
	relFP, err := filepath.Rel(path2, path1)
	if err != nil {
		return true, err
	}

	// If the relative path contains "..", this function will
	// return false, because it is a cousin path. Only children (recursive)
	// and the path itself will return true.
	return !strings.Contains(relFP, ".."), nil
}
