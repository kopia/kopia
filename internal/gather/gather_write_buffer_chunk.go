package gather

import (
	"sync"
)

const chunkSize = 1 << 20 // 1MB chunks

var (
	freeListMutex         sync.Mutex
	freeList              [][]byte
	freeListHighWaterMark int
)

func allocChunk() []byte {
	freeListMutex.Lock()
	defer freeListMutex.Unlock()

	l := len(freeList)
	if l == 0 {
		return make([]byte, 0, chunkSize)
	}

	ch := freeList[l-1]
	freeList = freeList[0 : l-1]

	return ch
}

func releaseChunk(s []byte) {
	if cap(s) != chunkSize {
		return
	}

	freeListMutex.Lock()
	defer freeListMutex.Unlock()

	freeList = append(freeList, s[:0])
	if len(freeList) > freeListHighWaterMark {
		freeListHighWaterMark = len(freeList)
	}
}
