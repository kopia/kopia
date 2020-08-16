// Package server implements Kopia API server handlers.
package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.GetContextLoggerFunc("kopia/server")

const maintenanceAttemptFrequency = 10 * time.Minute

type apiRequestFunc func(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError)

// Server exposes simple HTTP API for programmatically accessing Kopia features.
type Server struct {
	OnShutdown func(ctx context.Context) error

	options   Options
	rep       repo.Repository
	cancelRep context.CancelFunc

	// all API requests run with shared lock on this mutex
	// administrative actions run with an exclusive lock and block API calls.
	mu              sync.RWMutex
	sourceManagers  map[snapshot.SourceInfo]*sourceManager
	uploadSemaphore chan struct{}
}

// APIHandlers handles API requests.
func (s *Server) APIHandlers() http.Handler {
	m := mux.NewRouter()

	// sources
	m.HandleFunc("/api/v1/sources", s.handleAPI(s.handleSourcesList)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/sources", s.handleAPI(s.handleSourcesCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/sources/upload", s.handleAPI(s.handleUpload)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/sources/cancel", s.handleAPI(s.handleCancel)).Methods(http.MethodPost)

	// snapshots
	m.HandleFunc("/api/v1/snapshots", s.handleAPI(s.handleSnapshotList)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/policy", s.handleAPI(s.handlePolicyGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/policy", s.handleAPI(s.handlePolicyPut)).Methods(http.MethodPut)
	m.HandleFunc("/api/v1/policy", s.handleAPI(s.handlePolicyDelete)).Methods(http.MethodDelete)

	m.HandleFunc("/api/v1/policies", s.handleAPI(s.handlePolicyList)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/refresh", s.handleAPI(s.handleRefresh)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/flush", s.handleAPI(s.handleFlush)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/shutdown", s.handleAPIPossiblyNotConnected(s.handleShutdown)).Methods(http.MethodPost)

	m.HandleFunc("/api/v1/objects/{objectID}", s.handleObjectGet).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/repo/status", s.handleAPIPossiblyNotConnected(s.handleRepoStatus)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/repo/connect", s.handleAPIPossiblyNotConnected(s.handleRepoConnect)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/create", s.handleAPIPossiblyNotConnected(s.handleRepoCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/disconnect", s.handleAPI(s.handleRepoDisconnect)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/algorithms", s.handleAPIPossiblyNotConnected(s.handleRepoSupportedAlgorithms)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/repo/sync", s.handleAPI(s.handleRepoSync)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/parameters", s.handleAPI(s.handleRepoParameters)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(s.handleContentInfo)).Methods(http.MethodGet).Queries("info", "1")
	m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(s.handleContentGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(s.handleContentPut)).Methods(http.MethodPut)

	m.HandleFunc("/api/v1/manifests/{manifestID}", s.handleAPI(s.handleManifestGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/manifests/{manifestID}", s.handleAPI(s.handleManifestDelete)).Methods(http.MethodDelete)
	m.HandleFunc("/api/v1/manifests", s.handleAPI(s.handleManifestCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/manifests", s.handleAPI(s.handleManifestList)).Methods(http.MethodGet)

	return m
}

func (s *Server) handleAPI(f apiRequestFunc) http.HandlerFunc {
	return s.handleAPIPossiblyNotConnected(func(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
		if s.rep == nil {
			return nil, requestError(serverapi.ErrorNotConnected, "not connected")
		}

		return f(ctx, r, body)
	})
}

func (s *Server) handleAPIPossiblyNotConnected(f apiRequestFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// we must pre-read request body before acquiring the lock as it sometimes leads to deadlock
		// in HTTP/2 server.
		// See https://github.com/golang/go/issues/40816
		body, berr := ioutil.ReadAll(r.Body)
		if berr != nil {
			http.Error(w, "error reading request body", http.StatusInternalServerError)
			return
		}

		s.mu.RLock()
		defer s.mu.RUnlock()

		ctx := r.Context()

		log(ctx).Debugf("request %v (%v bytes)", r.URL, len(body))

		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")

		v, err := f(ctx, r, body)

		if err == nil {
			if b, ok := v.([]byte); ok {
				if _, err := w.Write(b); err != nil {
					log(ctx).Warningf("error writing response: %v", err)
				}
			} else if err := e.Encode(v); err != nil {
				log(ctx).Warningf("error encoding response: %v", err)
			}

			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(err.httpErrorCode)
		log(ctx).Debugf("error code %v message %v", err.apiErrorCode, err.message)

		_ = e.Encode(&serverapi.ErrorResponse{
			Code:  err.apiErrorCode,
			Error: err.message,
		})
	}
}

func (s *Server) handleRefresh(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if err := s.rep.Refresh(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleFlush(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if err := s.rep.Flush(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleShutdown(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	log(ctx).Infof("shutting down due to API request")

	if f := s.OnShutdown; f != nil {
		go func() {
			if err := f(ctx); err != nil {
				log(ctx).Warningf("shutdown failed: %v", err)
			}
		}()
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) forAllSourceManagersMatchingURLFilter(ctx context.Context, c func(s *sourceManager, ctx context.Context) serverapi.SourceActionResponse, values url.Values) (interface{}, *apiError) {
	resp := &serverapi.MultipleSourceActionResponse{
		Sources: map[string]serverapi.SourceActionResponse{},
	}

	for src, mgr := range s.sourceManagers {
		if !sourceMatchesURLFilter(src, values) {
			continue
		}

		resp.Sources[src.String()] = c(mgr, ctx)
	}

	return resp, nil
}

func (s *Server) handleUpload(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter(ctx, (*sourceManager).upload, r.URL.Query())
}

func (s *Server) handleCancel(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	return s.forAllSourceManagersMatchingURLFilter(ctx, (*sourceManager).cancel, r.URL.Query())
}

func (s *Server) beginUpload(ctx context.Context, src snapshot.SourceInfo) {
	log(ctx).Debugf("waiting on semaphore to upload %v", src)
	s.uploadSemaphore <- struct{}{}

	log(ctx).Debugf("entered semaphore to upload %v", src)
}

func (s *Server) endUpload(ctx context.Context, src snapshot.SourceInfo) {
	log(ctx).Debugf("finished uploading %v", src)
	<-s.uploadSemaphore
}

// SetRepository sets the repository (nil is allowed and indicates server that is not
// connected to the repository).
func (s *Server) SetRepository(ctx context.Context, rep repo.Repository) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rep == rep {
		// nothing to do
		return nil
	}

	if s.rep != nil {
		// close previous source managers
		log(ctx).Infof("stopping all source managers")
		s.stopAllSourceManagersLocked(ctx)
		log(ctx).Infof("stopped all source managers")

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
		s.stopAllSourceManagersLocked(ctx)
		s.rep = nil

		return err
	}

	ctx, s.cancelRep = context.WithCancel(ctx)
	go s.refreshPeriodically(ctx, rep)
	go s.periodicMaintenance(ctx, rep)

	return nil
}

func (s *Server) refreshPeriodically(ctx context.Context, r repo.Repository) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(s.options.RefreshInterval):
			if err := r.Refresh(ctx); err != nil {
				log(ctx).Warningf("error refreshing repository: %v", err)
			}

			if err := s.SyncSources(ctx); err != nil {
				log(ctx).Warningf("unable to sync sources: %v", err)
			}
		}
	}
}

func (s *Server) periodicMaintenance(ctx context.Context, r repo.Repository) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(maintenanceAttemptFrequency):
			if err := snapshotmaintenance.Run(ctx, r, maintenance.ModeAuto, false); err != nil {
				log(ctx).Warningf("unable to run maintenance: %v", err)
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
func (s *Server) StopAllSourceManagers(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopAllSourceManagersLocked(ctx)
}

func (s *Server) stopAllSourceManagersLocked(ctx context.Context) {
	for _, sm := range s.sourceManagers {
		sm.stop(ctx)
	}

	for _, sm := range s.sourceManagers {
		sm.waitUntilStopped(ctx)
	}

	s.sourceManagers = map[snapshot.SourceInfo]*sourceManager{}
}

func (s *Server) syncSourcesLocked(ctx context.Context) error {
	sources := map[snapshot.SourceInfo]bool{}

	if s.rep != nil {
		snapshotSources, err := snapshot.ListSources(ctx, s.rep)
		if err != nil {
			return errors.Wrap(err, "unable to list sources")
		}

		policies, err := policy.ListPolicies(ctx, s.rep)
		if err != nil {
			return errors.Wrap(err, "unable to list sources")
		}

		for _, ss := range snapshotSources {
			sources[ss] = true
		}

		for _, pol := range policies {
			if pol.Target().Path != "" && pol.Target().Host != "" && pol.Target().UserName != "" {
				sources[pol.Target()] = true
			}
		}
	}

	// copy existing sources to a map, from which we will remove sources that are found
	// in the repository
	oldSourceManagers := map[snapshot.SourceInfo]*sourceManager{}
	for k, v := range s.sourceManagers {
		oldSourceManagers[k] = v
	}

	for src := range sources {
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
		sm.stop(ctx)
	}

	for src, sm := range oldSourceManagers {
		sm.waitUntilStopped(ctx)
		delete(s.sourceManagers, src)
	}

	return nil
}

// Options encompasses all API server options.
type Options struct {
	ConfigFile      string
	ConnectOptions  *repo.ConnectOptions
	RefreshInterval time.Duration
}

// New creates a Server.
// The server will manage sources for a given username@hostname.
func New(ctx context.Context, options Options) (*Server, error) {
	s := &Server{
		options:         options,
		sourceManagers:  map[snapshot.SourceInfo]*sourceManager{},
		uploadSemaphore: make(chan struct{}, 1),
	}

	return s, nil
}
