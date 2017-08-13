package storagetesting

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/blob"
)

type mapStorage struct {
	data  map[string][]byte
	mutex sync.RWMutex
}

func (s *mapStorage) BlockSize(id string) (int64, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	d, ok := s.data[string(id)]
	if !ok {
		return 0, blob.ErrBlockNotFound
	}

	return int64(len(d)), nil
}

func (s *mapStorage) GetBlock(id string, offset, length int64) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, ok := s.data[string(id)]
	if ok {
		if length < 0 {
			return data, nil
		}

		data = data[offset:]
		if int(length) > len(data) {
			return data, nil
		}
		return data[0:length], nil
	}

	return nil, blob.ErrBlockNotFound
}

func (s *mapStorage) PutBlock(id string, data []byte, options blob.PutOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[string(id)]; ok {
		return nil
	}

	s.data[string(id)] = append([]byte(nil), data...)
	return nil
}

func (s *mapStorage) DeleteBlock(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, string(id))
	return nil
}

func (s *mapStorage) ListBlocks(prefix string) (chan blob.BlockMetadata, blob.CancelFunc) {
	ch := make(chan blob.BlockMetadata)
	cancelled := make(chan bool)
	fixedTime := time.Now()
	go func() {
		defer close(ch)
		s.mutex.RLock()
		defer s.mutex.RUnlock()

		keys := []string{}
		for k := range s.data {
			if strings.HasPrefix(k, string(prefix)) {
				keys = append(keys, k)
			}
		}

		sort.Strings(keys)

		for _, k := range keys {
			v := s.data[k]
			select {
			case <-cancelled:
				return
			case ch <- blob.BlockMetadata{
				BlockID:   string(k),
				Length:    int64(len(v)),
				TimeStamp: fixedTime,
			}:
			}
		}
	}()
	return ch, func() {
		close(cancelled)
	}
}

func (s *mapStorage) Close() error {
	return nil
}

// NewMapStorage returns an implementation of Storage backed by the contents of given map.
// Used primarily for testing.
func NewMapStorage(data map[string][]byte) blob.Storage {
	return &mapStorage{data: data}
}
