// Package server implements Kopia API server handlers.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"time"

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

	options   Options
	rep       *repo.Repository
	cancelRep context.CancelFunc

	// all API requests run with shared lock on this mutex
	// administrative actions run with an exclusive lock and block API calls.
	mu              sync.RWMutex
	sourceManagers  map[snapshot.SourceInfo]*sourceManager
	uploadSemaphore chan struct{}
}

// APIHandlers handles API requests.
func (s *Server) APIHandlers() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/sources", s.handleAPI(s.handleSourcesList, "GET"))
	mux.HandleFunc("/api/v1/snapshots", s.handleAPI(s.handleSourceSnapshotList, "GET"))
	mux.HandleFunc("/api/v1/policies", s.handleAPI(s.handlePolicyList, "GET"))
	mux.HandleFunc("/api/v1/policy", s.handleAPI(s.handlePolicyCRUD, "GET", "PUT", "DELETE"))

	mux.HandleFunc("/api/v1/refresh", s.handleAPI(s.handleRefresh, "POST"))
	mux.HandleFunc("/api/v1/flush", s.handleAPI(s.handleFlush, "POST"))
	mux.HandleFunc("/api/v1/shutdown", s.handleAPIPossiblyNotConnected(s.handleShutdown, "POST"))

	mux.HandleFunc("/api/v1/sources/pause", s.handleAPI(s.handlePause, "POST"))
	mux.HandleFunc("/api/v1/sources/resume", s.handleAPI(s.handleResume, "POST"))
	mux.HandleFunc("/api/v1/sources/upload", s.handleAPI(s.handleUpload, "POST"))
	mux.HandleFunc("/api/v1/sources/cancel", s.handleAPI(s.handleCancel, "POST"))

	mux.HandleFunc("/api/v1/objects/", s.handleObjectGet)

	mux.HandleFunc("/api/v1/repo/status", s.handleAPIPossiblyNotConnected(s.handleRepoStatus, "GET"))
	mux.HandleFunc("/api/v1/repo/connect", s.handleAPIPossiblyNotConnected(s.handleRepoConnect, "POST"))
	mux.HandleFunc("/api/v1/repo/create", s.handleAPIPossiblyNotConnected(s.handleRepoCreate, "POST"))
	mux.HandleFunc("/api/v1/repo/disconnect", s.handleAPI(s.handleRepoDisconnect, "POST"))
	mux.HandleFunc("/api/v1/repo/algorithms", s.handleAPIPossiblyNotConnected(s.handleRepoSupportedAlgorithms, "GET"))
	mux.HandleFunc("/api/v1/repo/sync", s.handleAPI(s.handleRepoSync, "POST"))

	return mux
}

func (s *Server) handleAPI(f func(ctx context.Context, r *http.Request) (interface{}, *apiError), httpMethods ...string) http.HandlerFunc {
	return s.handleAPIPossiblyNotConnected(func(ctx context.Context, r *http.Request) (interface{}, *apiError) {
		if s.rep == nil {
			return nil, requestError(serverapi.ErrorNotConnected, "not connected")
		}

		return f(ctx, r)
	}, httpMethods...)
}

func (s *Server) handleAPIPossiblyNotConnected(f func(ctx context.Context, r *http.Request) (interface{}, *apiError), httpMethods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		log.Debug("request %v", r.URL)

		methodOK := false

		for _, m := range httpMethods {
			if r.Method == m {
				methodOK = true
				break
			}
		}

		if !methodOK {
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

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(err.httpErrorCode)
		log.Debug("error code %v message %v", err.apiErrorCode, err.message)

		_ = e.Encode(&serverapi.ErrorResponse{
			Code:  err.apiErrorCode,
			Error: err.message,
		})
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

	if f := s.OnShutdown; f != nil {
		go func() {
			if err := f(ctx); err != nil {
				log.Warningf("shutdown failed: %v", err)
			}
		}()
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
	log.Debugf("waiting on semaphore to upload %v", src)
	s.uploadSemaphore <- struct{}{}

	log.Debugf("entered semaphore to upload %v", src)
}

func (s *Server) endUpload(src snapshot.SourceInfo) {
	log.Debugf("finished uploading %v", src)
	<-s.uploadSemaphore
}

// SetRepository sets the repository (nil is allowed and indicates server that is not
// connected to the repository).
func (s *Server) SetRepository(ctx context.Context, rep *repo.Repository) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rep == rep {
		// nothing to do
		return nil
	}

	if s.rep != nil {
		// close previous source managers
		log.Infof("stopping all source managers")
		s.stopAllSourceManagersLocked()
		log.Infof("stopped all source managers")

		if err := s.rep.Close(ctx); err != nil {
			return errors.Wrap(err, "unable to close previous repository")
		}

		cr := s.cancelRep
		s.cancelRep = nil

		if cr != nil {
			cr()
		}
	}

	s.rep = rep
	if s.rep == nil {
		return nil
	}

	if err := s.syncSourcesLocked(ctx); err != nil {
		s.stopAllSourceManagersLocked()
		s.rep = nil

		return err
	}

	ctx, s.cancelRep = context.WithCancel(ctx)
	go s.refreshPeriodically(ctx, rep)

	return nil
}

func (s *Server) refreshPeriodically(ctx context.Context, r *repo.Repository) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(s.options.RefreshInterval):
			if err := r.Refresh(ctx); err != nil {
				log.Warningf("error refreshing repository: %v", err)
			}

			if err := s.syncSourcesLocked(ctx); err != nil {
				log.Warningf("unable to sync sources: %v", err)
			}
		}
	}
}

// SyncSources synchronizes the repository and source managers.
func (s *Server) SyncSources(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.syncSourcesLocked(ctx)
}

// StopAllSourceManagers causes all source managers to stop.
func (s *Server) StopAllSourceManagers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopAllSourceManagersLocked()
}

func (s *Server) stopAllSourceManagersLocked() {
	for _, sm := range s.sourceManagers {
		sm.stop()
	}

	for _, sm := range s.sourceManagers {
		sm.waitUntilStopped()
	}

	s.sourceManagers = map[snapshot.SourceInfo]*sourceManager{}
}

func (s *Server) syncSourcesLocked(ctx context.Context) error {
	sources, err := snapshot.ListSources(ctx, s.rep)
	if err != nil {
		return errors.Wrap(err, "unable to list sources")
	}

	// copy existing sources to a map, from which we will remove sources that are found
	// in the repository
	oldSourceManagers := map[snapshot.SourceInfo]*sourceManager{}
	for k, v := range s.sourceManagers {
		oldSourceManagers[k] = v
	}

	for _, src := range sources {
		if _, ok := oldSourceManagers[src]; ok {
			// pre-existing source, already has a manager
			delete(oldSourceManagers, src)
		} else {
			sm := newSourceManager(src, s)
			s.sourceManagers[src] = sm

			go sm.run(ctx)
		}
	}

	// whatever is left in oldSourceManagers are managers for sources that don't exist anymore.
	// stop source manager for sources no longer in the repo.
	for _, sm := range oldSourceManagers {
		sm.stop()
	}

	for src, sm := range oldSourceManagers {
		sm.waitUntilStopped()
		delete(s.sourceManagers, src)
	}

	return nil
}

// Options encompasses all API server options.
type Options struct {
	ConfigFile      string
	Hostname        string
	Username        string
	ConnectOptions  *repo.ConnectOptions
	RefreshInterval time.Duration
}

// New creates a Server on top of a given Repository.
// The server will manage sources for a given username@hostname.
func New(ctx context.Context, rep *repo.Repository, options Options) (*Server, error) {
	s := &Server{
		options:         options,
		sourceManagers:  map[snapshot.SourceInfo]*sourceManager{},
		uploadSemaphore: make(chan struct{}, 1),
	}

	return s, nil
}
