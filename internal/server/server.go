package server

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/kopia/kopia/repo"

	"github.com/bmizerany/pat"
)

type Server struct {
	mu sync.RWMutex

	rep *repo.Repository
}

func (s *Server) APIHandlers() http.Handler {
	p := pat.New()
	p.Get("/api/v1/status", s.handleAPI(s.handleStatus))
	p.Get("/api/v1/sources", s.handleAPI(s.handleSourcesList))
	p.Get("/api/v1/snapshots", s.handleAPI(s.handleSourceSnapshotList))
	return p
}

func (s *Server) handleAPI(f func(r *http.Request) (interface{}, *apiError)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := f(r)
		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		if err == nil {
			e.Encode(v)
			return
		}

		http.Error(w, err.message, err.code)
	})
}

func New(rep *repo.Repository) *Server {
	return &Server{rep: rep}
}
