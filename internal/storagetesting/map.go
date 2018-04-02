package storagetesting

import (
	"context"
	"io"
	"io/ioutil"
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

func (s *mapStorage) GetBlock(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, ok := s.data[id]
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

func (s *mapStorage) PutBlock(ctx context.Context, id string, r io.Reader) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	if _, ok := s.data[id]; ok {
		return nil
	}

	s.keyTime[id] = time.Now()
	s.data[id] = append([]byte{}, data...)
	return nil
}

func (s *mapStorage) DeleteBlock(ctx context.Context, id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, id)
	return nil
}

func (s *mapStorage) ListBlocks(ctx context.Context, prefix string) <-chan storage.BlockMetadata {
	ch := make(chan storage.BlockMetadata)
	go func() {
		defer close(ch)
		s.mutex.RLock()
		defer s.mutex.RUnlock()

		keys := []string{}
		for k := range s.data {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}

		sort.Strings(keys)

		for _, k := range keys {
			v := s.data[k]
			select {
			case <-ctx.Done():
				return
			case ch <- storage.BlockMetadata{
				BlockID:   k,
				Length:    int64(len(v)),
				TimeStamp: s.keyTime[k],
			}:
			}
		}
	}()
	return ch
}

func (s *mapStorage) Close(ctx context.Context) error {
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
