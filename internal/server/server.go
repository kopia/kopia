// Package server implements Kopia API server handlers.
package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var log = logging.Module("kopia/server")

const (
	// maximum time between attempts to run maintenance.
	maxMaintenanceAttemptFrequency = 4 * time.Hour
	sleepOnMaintenanceError        = 30 * time.Minute

	kopiaAuthCookie         = "Kopia-Auth"
	kopiaAuthCookieTTL      = 1 * time.Minute
	kopiaAuthCookieAudience = "kopia"
	kopiaAuthCookieIssuer   = "kopia-server"
)

type apiRequestFunc func(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError)

// Server exposes simple HTTP API for programmatically accessing Kopia features.
type Server struct {
	OnShutdown func(ctx context.Context) error

	options   Options
	rep       repo.Repository
	cancelRep context.CancelFunc

	authenticator auth.Authenticator
	authorizer    auth.Authorizer

	// all API requests run with shared lock on this mutex
	// administrative actions run with an exclusive lock and block API calls.
	mu              sync.RWMutex
	sourceManagers  map[snapshot.SourceInfo]*sourceManager
	mounts          sync.Map // object.ID -> mount.Controller
	uploadSemaphore chan struct{}

	taskmgr *uitask.Manager

	authCookieSigningKey []byte

	grpcServerState
}

// APIHandlers handles API requests.
func (s *Server) APIHandlers(legacyAPI bool) http.Handler {
	m := mux.NewRouter()

	// sources
	m.HandleFunc("/api/v1/sources", s.handleAPI(requireUIUser, s.handleSourcesList)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/sources", s.handleAPI(requireUIUser, s.handleSourcesCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/sources/upload", s.handleAPI(requireUIUser, s.handleUpload)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/sources/cancel", s.handleAPI(requireUIUser, s.handleCancel)).Methods(http.MethodPost)

	// snapshots
	m.HandleFunc("/api/v1/snapshots", s.handleAPI(requireUIUser, s.handleSnapshotList)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/policy", s.handleAPI(requireUIUser, s.handlePolicyGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/policy", s.handleAPI(requireUIUser, s.handlePolicyPut)).Methods(http.MethodPut)
	m.HandleFunc("/api/v1/policy", s.handleAPI(requireUIUser, s.handlePolicyDelete)).Methods(http.MethodDelete)
	m.HandleFunc("/api/v1/policy/resolve", s.handleAPI(requireUIUser, s.handlePolicyResolve)).Methods(http.MethodPost)

	m.HandleFunc("/api/v1/policies", s.handleAPI(requireUIUser, s.handlePolicyList)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/refresh", s.handleAPI(anyAuthenticatedUser, s.handleRefresh)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/shutdown", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleShutdown)).Methods(http.MethodPost)

	m.HandleFunc("/api/v1/objects/{objectID}", s.requireAuth(s.handleObjectGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/restore", s.handleAPI(requireUIUser, s.handleRestore)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/estimate", s.handleAPI(requireUIUser, s.handleEstimate)).Methods(http.MethodPost)

	// path APIs
	m.HandleFunc("/api/v1/paths/resolve", s.handleAPI(requireUIUser, s.handlePathResolve)).Methods(http.MethodPost)

	// path APIs
	m.HandleFunc("/api/v1/cli", s.handleAPI(requireUIUser, s.handleCLIInfo)).Methods(http.MethodGet)

	// methods that can be called by any authenticated user (UI or remote user).
	m.HandleFunc("/api/v1/flush", s.handleAPI(anyAuthenticatedUser, s.handleFlush)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/status", s.handleAPIPossiblyNotConnected(anyAuthenticatedUser, s.handleRepoStatus)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/repo/sync", s.handleAPI(anyAuthenticatedUser, s.handleRepoSync)).Methods(http.MethodPost)

	m.HandleFunc("/api/v1/repo/connect", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleRepoConnect)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/exists", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleRepoExists)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/create", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleRepoCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/description", s.handleAPI(requireUIUser, s.handleRepoSetDescription)).Methods(http.MethodPost)

	m.HandleFunc("/api/v1/repo/disconnect", s.handleAPI(requireUIUser, s.handleRepoDisconnect)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/repo/algorithms", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleRepoSupportedAlgorithms)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/repo/throttle", s.handleAPI(requireUIUser, s.handleRepoGetThrottle)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/repo/throttle", s.handleAPI(requireUIUser, s.handleRepoSetThrottle)).Methods(http.MethodPut)

	if legacyAPI {
		m.HandleFunc("/api/v1/repo/parameters", s.handleAPI(anyAuthenticatedUser, s.handleRepoParameters)).Methods(http.MethodGet)

		m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(requireContentAccess(auth.AccessLevelRead), s.handleContentInfo)).Methods(http.MethodGet).Queries("info", "1")
		m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(requireContentAccess(auth.AccessLevelRead), s.handleContentGet)).Methods(http.MethodGet)
		m.HandleFunc("/api/v1/contents/{contentID}", s.handleAPI(requireContentAccess(auth.AccessLevelAppend), s.handleContentPut)).Methods(http.MethodPut)

		m.HandleFunc("/api/v1/manifests/{manifestID}", s.handleAPI(handlerWillCheckAuthorization, s.handleManifestGet)).Methods(http.MethodGet)
		m.HandleFunc("/api/v1/manifests/{manifestID}", s.handleAPI(handlerWillCheckAuthorization, s.handleManifestDelete)).Methods(http.MethodDelete)
		m.HandleFunc("/api/v1/manifests", s.handleAPI(handlerWillCheckAuthorization, s.handleManifestCreate)).Methods(http.MethodPost)
		m.HandleFunc("/api/v1/manifests", s.handleAPI(handlerWillCheckAuthorization, s.handleManifestList)).Methods(http.MethodGet)
	}

	m.HandleFunc("/api/v1/mounts", s.handleAPI(requireUIUser, s.handleMountCreate)).Methods(http.MethodPost)
	m.HandleFunc("/api/v1/mounts/{rootObjectID}", s.handleAPI(requireUIUser, s.handleMountDelete)).Methods(http.MethodDelete)
	m.HandleFunc("/api/v1/mounts/{rootObjectID}", s.handleAPI(requireUIUser, s.handleMountGet)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/mounts", s.handleAPI(requireUIUser, s.handleMountList)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/current-user", s.handleAPIPossiblyNotConnected(requireUIUser, s.handleCurrentUser)).Methods(http.MethodGet)

	m.HandleFunc("/api/v1/tasks-summary", s.handleAPI(requireUIUser, s.handleTaskSummary)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/tasks", s.handleAPI(requireUIUser, s.handleTaskList)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/tasks/{taskID}", s.handleAPI(requireUIUser, s.handleTaskInfo)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/tasks/{taskID}/logs", s.handleAPI(requireUIUser, s.handleTaskLogs)).Methods(http.MethodGet)
	m.HandleFunc("/api/v1/tasks/{taskID}/cancel", s.handleAPI(requireUIUser, s.handleTaskCancel)).Methods(http.MethodPost)

	return m
}

func (s *Server) isAuthenticated(w http.ResponseWriter, r *http.Request) bool {
	if s.authenticator == nil {
		return true
	}

	username, password, ok := r.BasicAuth()
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Missing credentials.\n", http.StatusUnauthorized)

		return false
	}

	if c, err := r.Cookie(kopiaAuthCookie); err == nil && c != nil {
		if s.isAuthCookieValid(username, c.Value) {
			// found a short-term JWT cookie that matches given username, trust it.
			// this avoids potentially expensive password hashing inside the authenticator.
			return true
		}
	}

	if !s.authenticator.IsValid(r.Context(), s.rep, username, password) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Kopia"`)
		http.Error(w, "Access denied.\n", http.StatusUnauthorized)

		return false
	}

	now := clock.Now()

	ac, err := s.generateShortTermAuthCookie(username, now)
	if err != nil {
		log(r.Context()).Errorf("unable to generate short-term auth cookie: %v", err)
	} else {
		http.SetCookie(w, &http.Cookie{
			Name:    kopiaAuthCookie,
			Value:   ac,
			Expires: now.Add(kopiaAuthCookieTTL),
		})
	}

	return true
}

func (s *Server) isAuthCookieValid(username, cookieValue string) bool {
	tok, err := jwt.ParseWithClaims(cookieValue, &jwt.RegisteredClaims{}, func(t *jwt.Token) (interface{}, error) {
		return s.authCookieSigningKey, nil
	})
	if err != nil {
		return false
	}

	sc, ok := tok.Claims.(*jwt.RegisteredClaims)
	if !ok {
		return false
	}

	return sc.Subject == username
}

func (s *Server) generateShortTermAuthCookie(username string, now time.Time) (string, error) {
	// nolint:wrapcheck
	return jwt.NewWithClaims(jwt.SigningMethodHS256, &jwt.RegisteredClaims{
		Subject:   username,
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(kopiaAuthCookieTTL)),
		IssuedAt:  jwt.NewNumericDate(now),
		Audience:  jwt.ClaimStrings{kopiaAuthCookieAudience},
		ID:        uuid.New().String(),
		Issuer:    kopiaAuthCookieIssuer,
	}).SignedString(s.authCookieSigningKey)
}

func (s *Server) requireAuth(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isAuthenticated(w, r) {
			return
		}

		f(w, r)
	}
}

func (s *Server) httpAuthorizationInfo(ctx context.Context, r *http.Request) auth.AuthorizationInfo {
	// authentication already done
	userAtHost, _, _ := r.BasicAuth()

	authz := s.authorizer.Authorize(ctx, s.rep, userAtHost)
	if authz == nil {
		authz = auth.NoAccess()
	}

	return authz
}

type isAuthorizedFunc func(s *Server, r *http.Request) bool

func (s *Server) handleAPI(isAuthorized isAuthorizedFunc, f apiRequestFunc) http.HandlerFunc {
	return s.handleAPIPossiblyNotConnected(isAuthorized, func(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
		if s.rep == nil {
			return nil, requestError(serverapi.ErrorNotConnected, "not connected")
		}

		return f(ctx, r, body)
	})
}

// RequireUIUserAuth wraps the provided http.Handler to only allow UI user and return 403 otherwise.
func (s *Server) RequireUIUserAuth(hf http.Handler) http.Handler {
	return s.requireAuth(func(rw http.ResponseWriter, r *http.Request) {
		if !requireUIUser(s, r) {
			http.Error(rw, `UI Access denied. See https://github.com/kopia/kopia/issues/880#issuecomment-798421751 for more information.`, http.StatusForbidden)
			return
		}

		hf.ServeHTTP(rw, r)
	})
}

func (s *Server) handleAPIPossiblyNotConnected(isAuthorized isAuthorizedFunc, f apiRequestFunc) http.HandlerFunc {
	return s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		// we must pre-read request body before acquiring the lock as it sometimes leads to deadlock
		// in HTTP/2 server.
		// See https://github.com/golang/go/issues/40816
		body, berr := io.ReadAll(r.Body)
		if berr != nil {
			http.Error(w, "error reading request body", http.StatusInternalServerError)
			return
		}

		s.mu.RLock()
		defer s.mu.RUnlock()

		ctx := r.Context()

		if s.options.LogRequests {
			log(ctx).Debugf("request %v (%v bytes)", r.URL, len(body))
		}

		w.Header().Set("Content-Type", "application/json")
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")

		var v interface{}
		var err *apiError

		// process the request while ignoring the cancelation signal
		// to ensure all goroutines started by it won't be canceled
		// when the request finishes.
		ctx = ctxutil.Detach(ctx)

		if isAuthorized(s, r) {
			v, err = f(ctx, r, body)
		} else {
			err = accessDeniedError()
		}

		if err == nil {
			if b, ok := v.([]byte); ok {
				if _, err := w.Write(b); err != nil {
					log(ctx).Errorf("error writing response: %v", err)
				}
			} else if err := e.Encode(v); err != nil {
				log(ctx).Errorf("error encoding response: %v", err)
			}

			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(err.httpErrorCode)

		if s.options.LogRequests && err.apiErrorCode == serverapi.ErrorNotConnected {
			log(ctx).Debugf("%v: error code %v message %v", r.URL, err.apiErrorCode, err.message)
		}

		_ = e.Encode(&serverapi.ErrorResponse{
			Code:  err.apiErrorCode,
			Error: err.message,
		})
	})
}

// Refresh refreshes the state of the server in response to external signal (e.g. SIGHUP).
func (s *Server) Refresh(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.internalRefreshRLocked(ctx)
}

func (s *Server) internalRefreshRLocked(ctx context.Context) error {
	if s.rep == nil {
		return nil
	}

	if err := s.rep.Refresh(ctx); err != nil {
		return errors.Wrap(err, "unable to refresh repository")
	}

	if s.authenticator != nil {
		if err := s.authenticator.Refresh(ctx); err != nil {
			log(ctx).Errorf("unable to refresh authenticator: %v", err)
		}
	}

	if s.authorizer != nil {
		if err := s.authorizer.Refresh(ctx); err != nil {
			log(ctx).Errorf("unable to refresh authorizer: %v", err)
		}
	}

	// release shared lock so that SyncSources can acquire exclusive lock
	s.mu.RUnlock()
	err := s.SyncSources(ctx)
	s.mu.RLock()
	if err != nil {
		return errors.Wrap(err, "unable to sync sources")
	}

	return nil
}

func (s *Server) handleRefresh(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	// refresh is an alias for /repo/sync
	return s.handleRepoSync(ctx, r, body)
}

func (s *Server) handleFlush(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	rw, ok := s.rep.(repo.RepositoryWriter)
	if !ok {
		return nil, repositoryNotWritableError()
	}

	if err := rw.Flush(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleShutdown(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	log(ctx).Infof("shutting down due to API request")

	if f := s.OnShutdown; f != nil {
		go func() {
			if err := f(ctx); err != nil {
				log(ctx).Errorf("shutdown failed: %v", err)
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

func (s *Server) triggerRefreshSource(sourceInfo snapshot.SourceInfo) {
	sm := s.sourceManagers[sourceInfo]
	if sm == nil {
		return
	}

	select {
	case sm.refreshRequested <- struct{}{}:
	default:
	}
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
		s.unmountAll(ctx)

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

	if dr, ok := rep.(repo.DirectRepository); ok {
		go s.periodicMaintenance(ctx, dr)
	}

	return nil
}

func (s *Server) refreshPeriodically(ctx context.Context, r repo.Repository) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(s.options.RefreshInterval):
			if err := r.Refresh(ctx); err != nil {
				log(ctx).Errorf("error refreshing repository: %v", err)
			}

			if err := s.SyncSources(ctx); err != nil {
				log(ctx).Errorf("unable to sync sources: %v", err)
			}
		}
	}
}

func (s *Server) periodicMaintenance(ctx context.Context, rep repo.DirectRepository) {
	for {
		now := clock.Now()

		// this will return time == now or in the past if the maintenance is currently runnable
		// by the current user.
		nextMaintenanceTime, err := maintenance.TimeToAttemptNextMaintenance(ctx, rep, now.Add(maxMaintenanceAttemptFrequency))
		if err != nil {
			log(ctx).Debugw("unable to determine time till next maintenance", "error", err)
			time.Sleep(sleepOnMaintenanceError)

			continue
		}

		// maintenance is not due yet, sleep interruptibly
		if nextMaintenanceTime.After(now) {
			log(ctx).Debugw("sleeping until next maintenance attempt", "time", nextMaintenanceTime)

			select {
			case <-ctx.Done():
				return

				// we woke up after sleeping, do not run maintenance immediately, but re-check first,
				// we may have lost ownership or parameters may have changed.
			case <-time.After(nextMaintenanceTime.Sub(now)):
			}
		} else if err := s.taskmgr.Run(ctx, "Maintenance", "Periodic maintenance", func(ctx context.Context, _ uitask.Controller) error {
			return periodicMaintenanceOnce(ctx, rep)
		}); err != nil {
			log(ctx).Errorf("unable to run maintenance: %v", err)
		}
	}
}

func periodicMaintenanceOnce(ctx context.Context, rep repo.Repository) error {
	dr, ok := rep.(repo.DirectRepository)
	if !ok {
		return errors.Errorf("not a direct repository")
	}

	// nolint:wrapcheck
	return repo.DirectWriteSession(ctx, dr, repo.WriteSessionOptions{
		Purpose: "periodicMaintenanceOnce",
	}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
		// nolint:wrapcheck
		return snapshotmaintenance.Run(ctx, w, maintenance.ModeAuto, false, maintenance.SafetyFull)
	})
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
		if sm, ok := oldSourceManagers[src]; ok {
			// pre-existing source, already has a manager
			delete(oldSourceManagers, src)
			sm.refreshStatus(ctx)
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
	ConfigFile           string
	ConnectOptions       *repo.ConnectOptions
	RefreshInterval      time.Duration
	MaxConcurrency       int
	Authenticator        auth.Authenticator
	Authorizer           auth.Authorizer
	PasswordPersist      passwordpersist.Strategy
	AuthCookieSigningKey string
	LogRequests          bool
	UIUser               string // name of the user allowed to access the UI
}

// New creates a Server.
// The server will manage sources for a given username@hostname.
func New(ctx context.Context, options Options) (*Server, error) {
	if options.Authorizer == nil {
		return nil, errors.Errorf("missing authorizer")
	}

	if options.PasswordPersist == nil {
		return nil, errors.Errorf("missing password persistence")
	}

	if options.AuthCookieSigningKey == "" {
		// generate random signing key
		options.AuthCookieSigningKey = uuid.New().String()
		log(ctx).Debugf("generated random auth cookie signing key: %v", options.AuthCookieSigningKey)
	}

	s := &Server{
		options:              options,
		sourceManagers:       map[snapshot.SourceInfo]*sourceManager{},
		uploadSemaphore:      make(chan struct{}, 1),
		grpcServerState:      makeGRPCServerState(options.MaxConcurrency),
		authenticator:        options.Authenticator,
		authorizer:           options.Authorizer,
		taskmgr:              uitask.NewManager(),
		authCookieSigningKey: []byte(options.AuthCookieSigningKey),
	}

	return s, nil
}
