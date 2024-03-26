package cachefs

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/testlogging"
)

const expirationTime = 10 * time.Hour

type cacheSource struct {
	data        map[string][]fs.Entry
	callCounter map[string]int
}

func identityWrapper(e fs.Entry) fs.Entry {
	return e
}

func (cs *cacheSource) get(id string) func(ctx context.Context) ([]fs.Entry, error) {
	return func(context.Context) ([]fs.Entry, error) {
		cs.callCounter[id]++

		d, ok := cs.data[id]
		if !ok {
			return nil, errors.New("no such id")
		}

		return d, nil
	}
}

func (cs *cacheSource) setEntryCount(id string, cnt int) {
	var fakeEntries []fs.Entry

	var fakeEntry fs.Entry

	for range cnt {
		fakeEntries = append(fakeEntries, fakeEntry)
	}

	cs.data[id] = fakeEntries
	cs.callCounter[id] = 0
}

func newCacheSource() *cacheSource {
	return &cacheSource{
		data:        make(map[string][]fs.Entry),
		callCounter: make(map[string]int),
	}
}

type cacheVerifier struct {
	cache           *Cache
	cacheSource     *cacheSource
	lastCallCounter map[string]int
}

func (cv *cacheVerifier) verifyCacheMiss(t *testing.T, id string) {
	t.Helper()

	actual := cv.cacheSource.callCounter[id]
	expected := cv.lastCallCounter[id] + 1

	if actual != expected {
		t.Errorf(errorPrefix()+"invalid call counter for %v, got %v, expected %v", id, actual, expected)
	}

	cv.reset()
}

func (cv *cacheVerifier) verifyCacheHit(t *testing.T, id string) {
	t.Helper()

	if !reflect.DeepEqual(cv.lastCallCounter, cv.cacheSource.callCounter) {
		t.Errorf(errorPrefix()+" unexpected call counters for %v, got %v, expected %v", id, cv.cacheSource.callCounter, cv.lastCallCounter)
	}

	cv.reset()
}

func (cv *cacheVerifier) verifyCacheOrdering(t *testing.T, expectedOrdering ...string) {
	t.Helper()

	var (
		actualOrdering                          []string
		totalDirectoryEntries, totalDirectories int
	)

	for e := cv.cache.head; e != nil; e = e.next {
		actualOrdering = append(actualOrdering, e.id)
		totalDirectoryEntries += len(e.entries)
		totalDirectories++
	}

	if cv.cache.totalDirectoryEntries != totalDirectoryEntries {
		t.Errorf("invalid totalDirectoryEntries: %v, expected %v", cv.cache.totalDirectoryEntries, totalDirectoryEntries)
	}

	if len(cv.cache.data) != totalDirectories {
		t.Errorf("invalid total directories: %v, expected %v", len(cv.cache.data), totalDirectories)
	}

	if !reflect.DeepEqual(actualOrdering, expectedOrdering) {
		t.Errorf(errorPrefix()+"unexpected ordering: %v, expected: %v", actualOrdering, expectedOrdering)
	}

	if totalDirectories > cv.cache.maxDirectories {
		t.Errorf(errorPrefix()+"total directories exceeds limit: %v, expected %v", totalDirectories, cv.cache.maxDirectories)
	}

	if totalDirectoryEntries > cv.cache.maxDirectoryEntries {
		t.Errorf(errorPrefix()+"total directory entries exceeds limit: %v, expected %v", totalDirectoryEntries, cv.cache.maxDirectoryEntries)
	}
}

func errorPrefix() string {
	if _, fn, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf("called from %v:%v: ", filepath.Base(fn), line)
	}

	return ""
}

func (cv *cacheVerifier) reset() {
	cv.lastCallCounter = make(map[string]int)
	for k, v := range cv.cacheSource.callCounter {
		cv.lastCallCounter[k] = v
	}
}

type lockState struct {
	l      sync.Locker
	locked atomic.Int32
}

// +checklocksacquire:ls.l
func (ls *lockState) Lock() {
	ls.l.Lock()

	ls.locked.Add(1)
}

// +checklocksrelease:ls.l
func (ls *lockState) Unlock() {
	ls.locked.Add(-1)
	ls.l.Unlock()
}

func (ls *lockState) Unlocked() bool {
	return ls.locked.Load() == 0
}

func TestCache(t *testing.T) {
	ctx := testlogging.Context(t)
	c := NewCache(&Options{
		MaxCachedDirectories: 4,
		MaxCachedEntries:     100,
	})

	if len(c.data) != 0 || c.totalDirectoryEntries != 0 || c.head != nil || c.tail != nil {
		t.Errorf("invalid initial state: %v %v %v %v", c.data, c.totalDirectoryEntries, c.head, c.tail)
	}

	cs := newCacheSource()
	cv := cacheVerifier{cacheSource: cs, cache: c}
	id1 := "1"
	id2 := "2"
	id3 := "3"
	id4 := "4"
	id5 := "5"
	id6 := "6"
	id7 := "7"

	cs.setEntryCount(id1, 3)
	cs.setEntryCount(id2, 3)
	cs.setEntryCount(id3, 3)
	cs.setEntryCount(id4, 95)
	cs.setEntryCount(id5, 70)
	cs.setEntryCount(id6, 100)
	cs.setEntryCount(id7, 101)

	cv.verifyCacheOrdering(t)

	// fetch id1
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheMiss(t, id1)
	cv.verifyCacheOrdering(t, id1)

	// fetch id1 again - cache hit, no change
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheHit(t, id1)
	cv.verifyCacheOrdering(t, id1)

	// fetch id2
	_, _ = c.getEntries(ctx, id2, expirationTime, cs.get(id2), identityWrapper)
	cv.verifyCacheMiss(t, id2)
	cv.verifyCacheOrdering(t, id2, id1)

	// fetch id1 again - cache hit, id1 moved to the top of the LRU list
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheHit(t, id1)
	cv.verifyCacheOrdering(t, id1, id2)

	// fetch id2 again
	_, _ = c.getEntries(ctx, id2, expirationTime, cs.get(id2), identityWrapper)
	cv.verifyCacheHit(t, id2)
	cv.verifyCacheOrdering(t, id2, id1)

	// fetch id3
	_, _ = c.getEntries(ctx, id3, expirationTime, cs.get(id3), identityWrapper)
	cv.verifyCacheMiss(t, id3)
	cv.verifyCacheOrdering(t, id3, id2, id1)

	// fetch id4
	_, _ = c.getEntries(ctx, id4, expirationTime, cs.get(id4), identityWrapper)
	cv.verifyCacheMiss(t, id4)
	cv.verifyCacheOrdering(t, id4, id3)

	// fetch id1 again
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheMiss(t, id1)
	cv.verifyCacheOrdering(t, id1, id4)

	// fetch id5, it's a big one that expels all but one
	_, _ = c.getEntries(ctx, id5, expirationTime, cs.get(id5), identityWrapper)
	cv.verifyCacheMiss(t, id5)
	cv.verifyCacheOrdering(t, id5, id1)

	// fetch id6
	_, _ = c.getEntries(ctx, id6, expirationTime, cs.get(id6), identityWrapper)
	cv.verifyCacheMiss(t, id6)
	cv.verifyCacheOrdering(t, id6)

	// fetch id7
	_, _ = c.getEntries(ctx, id7, expirationTime, cs.get(id7), identityWrapper)
	cv.verifyCacheMiss(t, id7)
	cv.verifyCacheOrdering(t, id6)
}

// Simple test for getEntries() locking/unlocking. Related to PRs #130 and #132.
func TestCacheGetEntriesLocking(t *testing.T) {
	ctx := testlogging.Context(t)
	c := NewCache(&Options{
		MaxCachedDirectories: 4,
		MaxCachedEntries:     100,
	})
	lock := &lockState{l: c.mu}
	c.mu = lock // allow checking the lock state below

	if len(c.data) != 0 || c.totalDirectoryEntries != 0 || c.head != nil || c.tail != nil {
		t.Errorf("invalid initial state: %v %v %v %v", c.data, c.totalDirectoryEntries, c.head, c.tail)
	}

	cs := newCacheSource()
	cv := cacheVerifier{cacheSource: cs, cache: c}

	const id1 = "1"

	cs.setEntryCount(id1, 1)

	// fetch non-existing entry, the loader will return an error
	actualEs, err := c.getEntries(ctx, id1, expirationTime, cs.get("foo"), identityWrapper)
	if err == nil {
		t.Fatal("Expected non-nil error when retrieving non-existing cache entry")
	}

	const expectedEsLength = 0

	actualEsLength := len(actualEs)
	if actualEsLength != expectedEsLength {
		t.Fatal("Expected empty entries, got: ", actualEsLength)
	}
	// cache must be unlocked at this point: See #130
	if !lock.Unlocked() {
		t.Fatal("Cache is locked after returning from getEntries")
	}

	// fetch id1
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheMiss(t, id1)
	// fetch id1 again - cache hit, no change
	_, _ = c.getEntries(ctx, id1, expirationTime, cs.get(id1), identityWrapper)
	cv.verifyCacheHit(t, id1)
	// cache must be unlocked and there should be no double unlock: See #132
	if !lock.Unlocked() {
		t.Fatal("Cache is locked after returning from getEntries")
	}
}
