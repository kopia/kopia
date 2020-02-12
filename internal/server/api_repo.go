package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

func (s *Server) handleRepoStatus(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep == nil {
		return &serverapi.StatusResponse{
			Connected: false,
		}, nil
	}

	return &serverapi.StatusResponse{
		Connected:   true,
		ConfigFile:  s.rep.ConfigFile,
		CacheDir:    s.rep.Content.CachingOptions.CacheDirectory,
		Hash:        s.rep.Content.Format.Hash,
		Encryption:  s.rep.Content.Format.Encryption,
		MaxPackSize: s.rep.Content.Format.MaxPackSize,
		Splitter:    s.rep.Objects.Format.Splitter,
		Storage:     s.rep.Blobs.ConnectionInfo().Type,
	}, nil
}

func (s *Server) handleRepoCreate(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError("already connected")
	}

	var req serverapi.CreateRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError("unable to decode request: " + err.Error())
	}

	st, err := blob.NewStorage(ctx, req.Storage)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to connect to storage"))
	}
	defer st.Close(ctx) //nolint:errcheck

	if err := repo.Initialize(ctx, st, &req.NewRepositoryOptions, req.Password); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to initialize repository"))
	}

	return s.connectAndOpen(ctx, req.Storage, req.Password)
}

func (s *Server) handleRepoConnect(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError("already connected")
	}

	var req serverapi.ConnectRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError("unable to decode request: " + err.Error())
	}

	return s.connectAndOpen(ctx, req.Storage, req.Password)
}

func (s *Server) connectAndOpen(ctx context.Context, conn blob.ConnectionInfo, password string) (interface{}, *apiError) {
	st, err := blob.NewStorage(ctx, conn)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "can't open storage"))
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = repo.Connect(ctx, s.options.ConfigFile, st, password, s.options.ConnectOptions); err != nil {
		return nil, internalServerError(errors.Wrap(err, "connect error"))
	}

	rep, err := repo.Open(ctx, s.options.ConfigFile, password, nil)
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "open error"))
	}

	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err = s.SetRepository(ctx, rep)
	s.mu.RLock()

	if err != nil {
		defer rep.Close(ctx) // nolint:errcheck
		return nil, internalServerError(err)
	}

	return s.handleRepoStatus(ctx, &http.Request{})
}

func (s *Server) handleRepoDisconnect(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep == nil {
		return nil, requestError("already disconnected")
	}

	if err := repo.Disconnect(s.options.ConfigFile); err != nil {
		return nil, internalServerError(err)
	}

	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err := s.SetRepository(ctx, nil)
	s.mu.RLock()

	if err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleRepoLock(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return nil, &apiError{code: http.StatusNotImplemented}
}

func (s *Server) handleRepoUnlock(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	return nil, &apiError{code: http.StatusNotImplemented}
}
