package storage

import (
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"
)

type mapRepository struct {
	data  map[string][]byte
	mutex sync.RWMutex
}

func (s *mapRepository) Configuration() RepositoryConfiguration {
	return RepositoryConfiguration{}
}

func (s *mapRepository) BlockExists(id BlockID) (bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, ok := s.data[string(id)]
	return ok, nil
}

func (s *mapRepository) GetBlock(id BlockID) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, ok := s.data[string(id)]
	if ok {
		return data, nil
	}

	return nil, ErrBlockNotFound
}

func (s *mapRepository) PutBlock(id BlockID, data io.ReadCloser, options PutOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	c, err := ioutil.ReadAll(data)
	data.Close()
	if err != nil {
		return err
	}

	s.data[string(id)] = c
	return nil
}

func (s *mapRepository) DeleteBlock(id BlockID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, string(id))
	return nil
}

func (s *mapRepository) ListBlocks(prefix BlockID) chan (BlockMetadata) {
	ch := make(chan (BlockMetadata))
	fixedTime := time.Now()
	go func() {
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
			ch <- BlockMetadata{
				BlockID:   BlockID(k),
				Length:    uint64(len(v)),
				TimeStamp: fixedTime,
			}
		}
		close(ch)
	}()
	return ch
}

func (s *mapRepository) Flush() error {
	return nil
}

// NewMapRepository returns an implementation of Repository backed by the contents of given map.
// Used primarily for testing.
func NewMapRepository(data map[string][]byte) Repository {
	return &mapRepository{data: data}
}
