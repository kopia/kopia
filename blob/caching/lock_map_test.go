// Package caching implements a caching wrapper around another Storage.
package caching

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLockMap(t *testing.T) {
	lockMap := newLockMap()
	var wg sync.WaitGroup

	// 100 goroutines competing for 10 buckets
	workers := 100
	lockedBlocks := 10
	workTime := 20 * time.Millisecond

	counters := make([]int, lockedBlocks)

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			b := i % lockedBlocks
			blockID := fmt.Sprintf("block-%v", b)
			for i := 0; i < 3; i++ {
				lockMap.Lock(blockID)
				if counters[b] != 0 {
					t.Errorf("*** multiple goroutines entered block %v", blockID)
				}
				counters[b]++
				time.Sleep(workTime)
				counters[b]--
				if counters[b] != 0 {
					t.Errorf("*** multiple goroutines entered block %v", blockID)
				}
				lockMap.Unlock(blockID)
			}
			defer wg.Done()
		}(i)
	}

	wg.Wait()
}
