package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/kopia/kopia/snapshot"

	"github.com/kopia/kopia/repo"

	"github.com/bmizerany/pat"
)

// Server exposes simple HTTP API for programmatically accessing Kopia features.
type Server struct {
	hostname        string
	username        string
	rep             *repo.Repository
	snapshotManager *snapshot.Manager
	policyManager   *snapshot.PolicyManager

	mu             sync.RWMutex
	sourceManagers map[snapshot.SourceInfo]*sourceManager
}

// APIHandlers handles API requests.
func (s *Server) APIHandlers() http.Handler {
	p := pat.New()
	p.Get("/api/v1/status", s.handleAPI(s.handleStatus))
	p.Get("/api/v1/sources", s.handleAPI(s.handleSourcesList))
	p.Get("/api/v1/snapshots", s.handleAPI(s.handleSourceSnapshotList))
	return p
}

func (s *Server) handleAPI(f func(r *http.Request) (interface{}, *apiError)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")

		v, err := f(r)
		if err == nil {
			e.Encode(v) //nolint:errcheck
			return
		}

		http.Error(w, err.message, err.code)
	})
}

// New creates a Server on top of a given Repository.
// The server will manage sources for a given username@hostname.
func New(ctx context.Context, rep *repo.Repository, hostname string, username string) (*Server, error) {
	s := &Server{
		hostname:        hostname,
		username:        username,
		rep:             rep,
		snapshotManager: snapshot.NewManager(rep),
		policyManager:   snapshot.NewPolicyManager(rep),
		sourceManagers:  map[snapshot.SourceInfo]*sourceManager{},
	}

	for _, src := range s.snapshotManager.ListSources() {
		sm := newSourceManager(src, s)
		s.sourceManagers[src] = sm
	}

	for _, src := range s.sourceManagers {
		go src.run()
	}

	return s, nil
}
