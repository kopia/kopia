package fuse

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/kopia/kopia/fs"
)

type cacheSource struct {
	data        map[int64]fs.Entries
	callCounter map[int64]int
}

func (cs *cacheSource) get(id int64) func() (fs.Entries, error) {
	return func() (fs.Entries, error) {
		cs.callCounter[id]++
		d, ok := cs.data[id]
		if !ok {
			return nil, errors.New("no such id")
		}

		return d, nil
	}
}

func (cs *cacheSource) setEntryCount(id int64, cnt int) {
	var fakeEntries fs.Entries
	var fakeEntry fs.Entry
	for i := 0; i < cnt; i++ {
		fakeEntries = append(fakeEntries, fakeEntry)
	}

	cs.data[id] = fakeEntries
	cs.callCounter[id] = 0
}

func newCacheSource() *cacheSource {
	return &cacheSource{
		data:        make(map[int64]fs.Entries),
		callCounter: make(map[int64]int),
	}
}

type cacheVerifier struct {
	cache           *Cache
	cacheSource     *cacheSource
	lastCallCounter map[int64]int
}

func (cv *cacheVerifier) verifyCacheMiss(t *testing.T, id int64) {
	actual := cv.cacheSource.callCounter[id]
	expected := cv.lastCallCounter[id] + 1
	if actual != expected {
		t.Errorf(errorPrefix()+"invalid call counter for %v, got %v, expected %v", id, actual, expected)
	}
	cv.reset()
}

func (cv *cacheVerifier) verifyCacheHit(t *testing.T, id int64) {
	if !reflect.DeepEqual(cv.lastCallCounter, cv.cacheSource.callCounter) {
		t.Errorf(errorPrefix()+" unexpected call counters for %v, got %v, expected %v", id, cv.cacheSource.callCounter, cv.lastCallCounter)
	}
	cv.reset()
}

func (cv *cacheVerifier) verifyCacheOrdering(t *testing.T, expectedOrdering ...int64) {
	var actualOrdering []int64
	var totalDirectoryEntries int
	var totalDirectories int
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
	cv.lastCallCounter = make(map[int64]int)
	for k, v := range cv.cacheSource.callCounter {
		cv.lastCallCounter[k] = v
	}
}

func TestCache(t *testing.T) {
	c := NewCache(
		MaxCachedDirectories(4),
		MaxCachedDirectoryEntries(100),
	)
	if len(c.data) != 0 || c.totalDirectoryEntries != 0 || c.head != nil || c.tail != nil {
		t.Errorf("invalid initial state: %v %v %v %v", c.data, c.totalDirectoryEntries, c.head, c.tail)
	}

	cs := newCacheSource()
	cv := cacheVerifier{cacheSource: cs, cache: c}
	var id1 int64 = 1
	var id2 int64 = 2
	var id3 int64 = 3
	var id4 int64 = 4
	var id5 int64 = 5
	var id6 int64 = 6
	var id7 int64 = 7
	cs.setEntryCount(id1, 3)
	cs.setEntryCount(id2, 3)
	cs.setEntryCount(id3, 3)
	cs.setEntryCount(id4, 95)
	cs.setEntryCount(id5, 70)
	cs.setEntryCount(id6, 100)
	cs.setEntryCount(id7, 101)

	cv.verifyCacheOrdering(t)

	// fetch id1
	c.getEntries(id1, cs.get(id1))
	cv.verifyCacheMiss(t, id1)
	cv.verifyCacheOrdering(t, id1)

	// fetch id1 again - cache hit, no change
	c.getEntries(id1, cs.get(id1))
	cv.verifyCacheHit(t, id1)
	cv.verifyCacheOrdering(t, id1)

	// fetch id2
	c.getEntries(id2, cs.get(id2))
	cv.verifyCacheMiss(t, id2)
	cv.verifyCacheOrdering(t, id2, id1)

	// fetch id1 again - cache hit, id1 moved to the top of the LRU list
	c.getEntries(id1, cs.get(id1))
	cv.verifyCacheHit(t, id1)
	cv.verifyCacheOrdering(t, id1, id2)

	// fetch id2 again
	c.getEntries(id2, cs.get(id2))
	cv.verifyCacheHit(t, id2)
	cv.verifyCacheOrdering(t, id2, id1)

	// fetch id3
	c.getEntries(id3, cs.get(id3))
	cv.verifyCacheMiss(t, id3)
	cv.verifyCacheOrdering(t, id3, id2, id1)

	// fetch id4
	c.getEntries(id4, cs.get(id4))
	cv.verifyCacheMiss(t, id4)
	cv.verifyCacheOrdering(t, id4, id3)

	// fetch id1 again
	c.getEntries(id1, cs.get(id1))
	cv.verifyCacheMiss(t, id1)
	cv.verifyCacheOrdering(t, id1, id4)

	// fetch id5, it's a big one that expels all but one
	c.getEntries(id5, cs.get(id5))
	cv.verifyCacheMiss(t, id5)
	cv.verifyCacheOrdering(t, id5, id1)

	// fetch id6
	c.getEntries(id6, cs.get(id6))
	cv.verifyCacheMiss(t, id6)
	cv.verifyCacheOrdering(t, id6)

	// fetch id7
	c.getEntries(id7, cs.get(id7))
	cv.verifyCacheMiss(t, id7)
	cv.verifyCacheOrdering(t, id6)
}
