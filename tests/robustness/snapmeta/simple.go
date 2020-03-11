package snapmeta

import "sync"

var _ Store = &Simple{}

// Simple is a snapstore implementation that stores
// snapshot metadata as a byte slice in a map in memory.
// A Simple should not be copied.
type Simple struct {
	m  map[string][]byte
	mu sync.Mutex
}

// NewSimple instantiates a new Simple snapstore and
// returns its pointer
func NewSimple() *Simple {
	return &Simple{
		m: make(map[string][]byte),
	}
}

// Store implements the Storer interface Store method
func (s *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key] = buf

	return nil
}

// Load implements the Storer interface Load method
func (s *Simple) Load(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if buf, found := s.m[key]; found {
		retBuf := make([]byte, len(buf))
		_ = copy(retBuf, buf)

		return retBuf, nil
	}

	return nil, nil
}

// Delete implements the Storer interface Delete method
func (s *Simple) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, key)
}

// GetKeys implements the Storer interface GetKeys method
func (s *Simple) GetKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make([]string, 0, len(s.m))

	for k := range s.m {
		ret = append(ret, k)
	}

	return ret
}
