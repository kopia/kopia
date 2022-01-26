package testenv

import (
	"encoding/json"
	"net/http"
	"time"
)

type fakeTimeInfoStruct struct {
	Time     time.Time     `json:"time"`
	ValidFor time.Duration `json:"validFor"`
}

// FakeTimeServer serves fake time signal to instances of Kopia.
type FakeTimeServer struct {
	Now func() time.Time
}

func (s *FakeTimeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(fakeTimeInfoStruct{
		Time:     s.Now(),
		ValidFor: 2 * time.Second,
	}) //nolint:errcheck,errchkjson
}

// NewFakeTimeServer creates new time server that serves time over HTTP and locally.
func NewFakeTimeServer(now func() time.Time) *FakeTimeServer {
	return &FakeTimeServer{
		Now: now,
	}
}

var _ http.Handler = (*FakeTimeServer)(nil)
