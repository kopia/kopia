package blobtesting

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/blob"
)

type DataMap map[blob.ID][]byte

type mapStorage struct {
	data    DataMap
	keyTime map[blob.ID]time.Time
	timeNow func() time.Time
	mutex   sync.RWMutex
}

func (s *mapStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
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

	return nil, blob.ErrBlobNotFound
}

func (s *mapStorage) PutBlob(ctx context.Context, id blob.ID, data []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[id]; ok {
		return nil
	}

	s.keyTime[id] = s.timeNow()
	s.data[id] = append([]byte{}, data...)
	return nil
}

func (s *mapStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, id)
	delete(s.keyTime, id)
	return nil
}

func (s *mapStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	s.mutex.RLock()

	keys := []blob.ID{}
	for k := range s.data {
		if strings.HasPrefix(string(k), string(prefix)) {
			keys = append(keys, k)
		}
	}
	s.mutex.RUnlock()

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		s.mutex.RLock()
		v, ok := s.data[k]
		ts := s.keyTime[k]
		s.mutex.RUnlock()
		if !ok {
			continue
		}
		if err := callback(blob.Metadata{
			BlobID:    k,
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

func (s *mapStorage) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if v, ok := s.keyTime[blobID]; ok {
		n := s.timeNow()
		if n.Sub(v) >= threshold {
			s.keyTime[blobID] = n
		}
	}

	return nil
}

func (s *mapStorage) ConnectionInfo() blob.ConnectionInfo {
	// unsupported
	return blob.ConnectionInfo{}
}

// NewMapStorage returns an implementation of Storage backed by the contents of given map.
// Used primarily for testing.
func NewMapStorage(data DataMap, keyTime map[blob.ID]time.Time, timeNow func() time.Time) blob.Storage {
	if keyTime == nil {
		keyTime = make(map[blob.ID]time.Time)
	}
	if timeNow == nil {
		timeNow = time.Now
	}
	return &mapStorage{data: data, keyTime: keyTime, timeNow: timeNow}
}
