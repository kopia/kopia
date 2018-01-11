package storagetesting

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/storage"
)

type mapStorage struct {
	data    map[string][]byte
	keyTime map[string]time.Time
	mutex   sync.RWMutex
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

	return nil, storage.ErrBlockNotFound
}

func (s *mapStorage) PutBlock(id string, data []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[string(id)]; ok {
		return nil
	}

	s.keyTime[id] = time.Now()
	s.data[id] = append([]byte{}, data...)
	return nil
}

func (s *mapStorage) DeleteBlock(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, string(id))
	return nil
}

func (s *mapStorage) ListBlocks(prefix string) (<-chan storage.BlockMetadata, storage.CancelFunc) {
	ch := make(chan storage.BlockMetadata)
	cancelled := make(chan bool)
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
			case ch <- storage.BlockMetadata{
				BlockID:   string(k),
				Length:    int64(len(v)),
				TimeStamp: s.keyTime[k],
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

func (s *mapStorage) ConnectionInfo() storage.ConnectionInfo {
	// unsupported
	return storage.ConnectionInfo{}
}

// NewMapStorage returns an implementation of Storage backed by the contents of given map.
// Used primarily for testing.
func NewMapStorage(data map[string][]byte, keyTime map[string]time.Time) storage.Storage {
	if keyTime == nil {
		keyTime = make(map[string]time.Time)
	}
	return &mapStorage{data: data, keyTime: keyTime}
}
