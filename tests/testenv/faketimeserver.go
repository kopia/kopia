package testenv

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type fakeTimeInfoStruct struct {
	Next  int64 `json:"next"`
	Step  int64 `json:"step"`
	Until int64 `json:"until"`
}

// FakeTimeServer serves fake time signal to instances of Kopia.
type FakeTimeServer struct {
	mu sync.Mutex

	nextTimeChunk   time.Time
	timeChunkLength time.Duration
	step            time.Duration
}

// Now returns current fake time.
func (s *FakeTimeServer) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := s.nextTimeChunk
	s.nextTimeChunk = s.nextTimeChunk.Add(s.step)

	return t
}

func (s *FakeTimeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeInfo := fakeTimeInfoStruct{
		Next:  s.nextTimeChunk.UnixNano(),
		Step:  s.step.Nanoseconds(),
		Until: s.nextTimeChunk.Add(s.timeChunkLength).UnixNano(),
	}

	s.nextTimeChunk = s.nextTimeChunk.Add(s.timeChunkLength)

	json.NewEncoder(w).Encode(timeInfo) //nolint:errcheck
}

// NewFakeTimeServer creates new time server that serves time over HTTP and locally.
func NewFakeTimeServer(startTime time.Time, step time.Duration) *FakeTimeServer {
	return &FakeTimeServer{
		nextTimeChunk:   startTime,
		timeChunkLength: 100 * step,
		step:            step,
	}
}

var _ http.Handler = (*FakeTimeServer)(nil)
