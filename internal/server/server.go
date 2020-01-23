// Package server implements Kopia API server handlers.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var log = kopialogging.Logger("kopia/server")

// Server exposes simple HTTP API for programmatically accessing Kopia features.
type Server struct {
	OnShutdown func(ctx context.Context) error

	hostname        string
	username        string
	rep             *repo.Repository
	mu              sync.RWMutex
	sourceManagers  map[snapshot.SourceInfo]*sourceManager
	uploadSemaphore chan struct{}
}

// APIHandlers handles API requests.
func (s *Server) APIHandlers() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/status", s.handleAPI(s.handleStatus, "GET"))
	mux.HandleFunc("/api/v1/sources", s.handleAPI(s.handleSourcesList, "GET"))
	mux.HandleFunc("/api/v1/snapshots", s.handleAPI(s.handleSourceSnapshotList, "GET"))
	mux.HandleFunc("/api/v1/policies", s.handleAPI(s.handlePolicyList, "GET"))
	mux.HandleFunc("/api/v1/refresh", s.handleAPI(s.handleRefresh, "POST"))
	mux.HandleFunc("/api/v1/flush", s.handleAPI(s.handleFlush, "POST"))
	mux.HandleFunc("/api/v1/shutdown", s.handleAPI(s.handleShutdown, "POST"))
	mux.HandleFunc("/api/v1/sources/pause", s.handleAPI(s.handlePause, "POST"))
	mux.HandleFunc("/api/v1/sources/resume", s.handleAPI(s.handleResume, "POST"))
	mux.HandleFunc("/api/v1/sources/upload", s.handleAPI(s.handleUpload, "POST"))
	mux.HandleFunc("/api/v1/sources/cancel", s.handleAPI(s.handleCancel, "POST"))
	mux.HandleFunc("/api/v1/objects/", s.handleObjectGet)

	return mux
}

func (s *Server) handleAPI(f func(ctx context.Context, r *http.Request) (interface{}, *apiError), httpMethod string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if r.Method != httpMethod {
			http.Error(w, "incompatible HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")

		v, err := f(context.Background(), r)

		if err == nil {
			if err := e.Encode(v); err != nil {
				log.Warningf("error encoding response: %v", err)
			}

			return
		}

		http.Error(w, err.message, err.code)
	}
}

func (s *Server) handleRefresh(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	log.Infof("refreshing")
	return &serverapi.Empty{}, nil
}

func (s *Server) handleFlush(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	log.Infof("flushing")
	return &serverapi.Empty{}, nil
}

func (s *Server) handleShutdown(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	log.Infof("shutting down due to API request")

	if s.OnShutdown != nil {
		if err := s.OnShutdown(ctx); err != nil {
			return nil, internalServerError(err)
		}
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) forAllSourceManagersMatchingURLFilter(c func(s *sourceManager) serverapi.SourceActionResponse, values url.Values) (interface{}, *apiError) {
	resp := &serverapi.MultipleSourceActionResponse{
		Sources: map[string]serverapi.SourceActionResponse{},
	}

	for src, mgr := range s.sourceManagers {
		if !sourceMatchesURLFilter(src, values) {
			continue
		}

		resp.Sources[src.String()] = c(mgr)
	}

	return resp, nil
}

func (s *Server) handleUpload(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter((*sourceManager).upload, r.URL.Query())
}

func (s *Server) handlePause(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter((*sourceManager).pause, r.URL.Query())
}

func (s *Server) handleResume(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter((*sourceManager).resume, r.URL.Query())
}

func (s *Server) handleCancel(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter((*sourceManager).cancel, r.URL.Query())
}

func (s *Server) beginUpload(src snapshot.SourceInfo) {
	log.Infof("waiting on semaphore to upload %v", src)
	s.uploadSemaphore <- struct{}{}

	log.Infof("entered semaphore to upload %v", src)
}

func (s *Server) endUpload(src snapshot.SourceInfo) {
	log.Infof("finished uploading %v", src)
	<-s.uploadSemaphore
}

// New creates a Server on top of a given Repository.
// The server will manage sources for a given username@hostname.
func New(ctx context.Context, rep *repo.Repository, hostname, username string) (*Server, error) {
	s := &Server{
		hostname:        hostname,
		username:        username,
		rep:             rep,
		sourceManagers:  map[snapshot.SourceInfo]*sourceManager{},
		uploadSemaphore: make(chan struct{}, 1),
	}

	sources, err := snapshot.ListSources(ctx, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	for _, src := range sources {
		sm := newSourceManager(src, s)
		s.sourceManagers[src] = sm
	}

	for _, src := range s.sourceManagers {
		go src.run(ctx)
	}

	return s, nil
}
