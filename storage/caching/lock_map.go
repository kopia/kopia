// Package caching implements a caching wrapper around another Storage.
package caching

import "sync"

type lockMap struct {
	cond  *sync.Cond
	locks map[string]bool
}

func (l *lockMap) getSync(id string) (*sync.Cond, map[string]bool) {
	return l.cond, l.locks
}

func (l *lockMap) Lock(id string) {
	cv, locks := l.getSync(id)

	cv.L.Lock()
	for locks[id] {
		cv.Wait()
	}
	locks[id] = true
	cv.L.Unlock()
}

func (l *lockMap) Unlock(id string) {
	cv, locks := l.getSync(id)

	cv.L.Lock()
	delete(locks, id)
	cv.Signal()
	cv.L.Unlock()
}

func newLockMap() *lockMap {
	return &lockMap{
		cond:  &sync.Cond{L: &sync.Mutex{}},
		locks: map[string]bool{},
	}
}
