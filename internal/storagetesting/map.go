package storagetesting

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/storage"
)

type mapStorage struct {
	data    map[string][]byte
	keyTime map[string]time.Time
	timeNow func() time.Time
	mutex   sync.RWMutex
}

func (s *mapStorage) GetBlock(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, ok := s.data[id]
	if ok {
		data = append([]byte(nil), data...)
		if length < 0 {
			return data, nil
		}

		if int(offset) > len(data) || offset < 0 {
			return nil, errors.New("invalid offset")
		}

		data = data[offset:]
		if int(length) > len(data) {
			return nil, errors.New("invalid length")
		}
		return data[0:length], nil
	}

	return nil, storage.ErrBlockNotFound
}

func (s *mapStorage) PutBlock(ctx context.Context, id string, data []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[id]; ok {
		return nil
	}

	s.keyTime[id] = s.timeNow()
	s.data[id] = append([]byte{}, data...)
	return nil
}

func (s *mapStorage) DeleteBlock(ctx context.Context, id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, id)
	delete(s.keyTime, id)
	return nil
}

func (s *mapStorage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	s.mutex.RLock()

	keys := []string{}
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	s.mutex.RUnlock()

	sort.Strings(keys)

	for _, k := range keys {
		s.mutex.RLock()
		v, ok := s.data[k]
		ts := s.keyTime[k]
		s.mutex.RUnlock()
		if !ok {
			continue
		}
		if err := callback(storage.BlockMetadata{
			BlockID:   k,
			Length:    int64(len(v)),
			Timestamp: ts,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapStorage) Close(ctx context.Context) error {
	return nil
}

func (s *mapStorage) TouchBlock(ctx context.Context, blockID string, threshold time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if v, ok := s.keyTime[blockID]; ok {
		n := s.timeNow()
		if n.Sub(v) >= threshold {
			s.keyTime[blockID] = n
		}
	}

	return nil
}

func (s *mapStorage) ConnectionInfo() storage.ConnectionInfo {
	// unsupported
	return storage.ConnectionInfo{}
}

// NewMapStorage returns an implementation of Storage backed by the contents of given map.
// Used primarily for testing.
func NewMapStorage(data map[string][]byte, keyTime map[string]time.Time, timeNow func() time.Time) storage.Storage {
	if keyTime == nil {
		keyTime = make(map[string]time.Time)
	}
	if timeNow == nil {
		timeNow = time.Now
	}
	return &mapStorage{data: data, keyTime: keyTime, timeNow: timeNow}
}
