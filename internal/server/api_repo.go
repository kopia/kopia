package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/policy"
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

func maybeDecodeToken(req *serverapi.ConnectRequest) *apiError {
	if req.Token != "" {
		ci, password, err := repo.DecodeToken(req.Token)
		if err != nil {
			return requestError(serverapi.ErrorInvalidToken, "invalid token: "+err.Error())
		}

		req.Storage = ci
		if password != "" {
			req.Password = password
		}
	}

	return nil
}

func (s *Server) handleRepoCreate(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.CreateRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	if err := maybeDecodeToken(&req.ConnectRequest); err != nil {
		return nil, err
	}

	st, err := blob.NewStorage(ctx, req.Storage)
	if err != nil {
		return nil, requestError(serverapi.ErrorStorageConnection, "unable to connect to storage: "+err.Error())
	}
	defer st.Close(ctx) //nolint:errcheck

	if err := repo.Initialize(ctx, st, &req.NewRepositoryOptions, req.Password); err != nil {
		return nil, repoErrorToAPIError(err)
	}

	if err := s.connectAndOpen(ctx, req.Storage, req.Password); err != nil {
		return nil, err
	}

	if err := policy.SetPolicy(ctx, s.rep, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
		return nil, internalServerError(errors.Wrap(err, "set global policy"))
	}

	if err := s.rep.Flush(ctx); err != nil {
		return nil, internalServerError(errors.Wrap(err, "flush"))
	}

	return s.handleRepoStatus(ctx, r)
}

func (s *Server) handleRepoConnect(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.ConnectRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	if err := maybeDecodeToken(&req); err != nil {
		return nil, err
	}

	if err := s.connectAndOpen(ctx, req.Storage, req.Password); err != nil {
		return nil, err
	}

	return s.handleRepoStatus(ctx, r)
}

func (s *Server) handleRepoSupportedAlgorithms(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	res := &serverapi.SupportedAlgorithmsResponse{
		DefaultHashAlgorithm: content.DefaultHash,
		HashAlgorithms:       content.SupportedHashAlgorithms(),

		DefaultEncryptionAlgorithm: content.DefaultEncryption,
		EncryptionAlgorithms:       content.SupportedEncryptionAlgorithms(),

		DefaultSplitterAlgorithm: object.DefaultSplitter,
		SplitterAlgorithms:       object.SupportedSplitters,
	}

	for k := range compression.ByName {
		res.CompressionAlgorithms = append(res.CompressionAlgorithms, string(k))
	}

	sort.Strings(res.CompressionAlgorithms)

	return res, nil
}

func (s *Server) connectAndOpen(ctx context.Context, conn blob.ConnectionInfo, password string) *apiError {
	st, err := blob.NewStorage(ctx, conn)
	if err != nil {
		return requestError(serverapi.ErrorStorageConnection, "can't open storage: "+err.Error())
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = repo.Connect(ctx, s.options.ConfigFile, st, password, s.options.ConnectOptions); err != nil {
		return repoErrorToAPIError(err)
	}

	rep, err := repo.Open(ctx, s.options.ConfigFile, password, nil)
	if err != nil {
		return repoErrorToAPIError(err)
	}

	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err = s.SetRepository(ctx, rep)
	s.mu.RLock()

	if err != nil {
		defer rep.Close(ctx) // nolint:errcheck
		return internalServerError(err)
	}

	return nil
}

func (s *Server) handleRepoDisconnect(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err := s.SetRepository(ctx, nil)
	s.mu.RLock()

	if err != nil {
		return nil, internalServerError(err)
	}

	if err := repo.Disconnect(s.options.ConfigFile); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleRepoSync(ctx context.Context, r *http.Request) (interface{}, *apiError) {
	if err := s.rep.Refresh(ctx); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to refresh repository"))
	}

	// release shared lock so that SyncSources can acquire exclusive lock
	s.mu.RUnlock()
	err := s.SyncSources(ctx)
	s.mu.RLock()
	if err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to sync sources"))
	}

	return &serverapi.Empty{}, nil
}

func repoErrorToAPIError(err error) *apiError {
	switch err {
	case repo.ErrRepositoryNotInitialized:
		return requestError(serverapi.ErrorNotInitialized, "repository not initialized")
	case repo.ErrInvalidPassword:
		return requestError(serverapi.ErrorInvalidPassword, "invalid password")
	case repo.ErrAlreadyInitialized:
		return requestError(serverapi.ErrorAlreadyInitialized, "repository already initialized")
	default:
		return internalServerError(errors.Wrap(err, "connect error"))
	}
}
