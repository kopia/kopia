package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

func (s *Server) handleRepoParameters(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(*repo.DirectRepository)
	if !ok {
		return &serverapi.StatusResponse{
			Connected: false,
		}, nil
	}

	rp := &remoterepoapi.Parameters{
		HashFunction: dr.Content.Format.Hash,
		HMACSecret:   dr.Content.Format.HMACSecret,
		Format:       dr.Objects.Format,
	}

	return rp, nil
}

func (s *Server) handleRepoStatus(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if s.rep == nil {
		return &serverapi.StatusResponse{
			Connected: false,
		}, nil
	}

	dr, ok := s.rep.(*repo.DirectRepository)
	if ok {
		return &serverapi.StatusResponse{
			Connected:     true,
			ConfigFile:    dr.ConfigFile,
			CacheDir:      dr.Content.CachingOptions.CacheDirectory,
			Hash:          dr.Content.Format.Hash,
			Encryption:    dr.Content.Format.Encryption,
			MaxPackSize:   dr.Content.Format.MaxPackSize,
			Splitter:      dr.Objects.Format.Splitter,
			Storage:       dr.Blobs.ConnectionInfo().Type,
			ClientOptions: dr.ClientOptions(),
		}, nil
	}

	type remoteRepository interface {
		APIServerURL() string
	}

	result := &serverapi.StatusResponse{
		Connected:     true,
		ClientOptions: s.rep.ClientOptions(),
	}

	if rr, ok := s.rep.(remoteRepository); ok {
		result.APIServerURL = rr.APIServerURL()
	}

	return result, nil
}

func maybeDecodeToken(req *serverapi.ConnectRepositoryRequest) *apiError {
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

func (s *Server) handleRepoCreate(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.CreateRepositoryRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	if err := maybeDecodeToken(&req.ConnectRepositoryRequest); err != nil {
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

	if err := s.connectAndOpen(ctx, req.Storage, req.Password, req.ClientOptions); err != nil {
		return nil, err
	}

	if err := policy.SetPolicy(ctx, s.rep, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
		return nil, internalServerError(errors.Wrap(err, "set global policy"))
	}

	if dr, ok := s.rep.(*repo.DirectRepository); ok {
		p := maintenance.DefaultParams()
		p.Owner = dr.Username() + "@" + dr.Hostname()

		if err := maintenance.SetParams(ctx, dr, &p); err != nil {
			return nil, internalServerError(errors.Wrap(err, "unable to set maintenance params"))
		}
	}

	if err := s.rep.Flush(ctx); err != nil {
		return nil, internalServerError(errors.Wrap(err, "flush"))
	}

	return s.handleRepoStatus(ctx, r, nil)
}

func (s *Server) handleRepoExists(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.CheckRepositoryExistsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	st, err := blob.NewStorage(ctx, req.Storage)
	if err != nil {
		return nil, internalServerError(err)
	}

	defer st.Close(ctx) // nolint:errcheck

	_, err = st.GetBlob(ctx, repo.FormatBlobID, 0, -1)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil, requestError(serverapi.ErrorNotInitialized, "repository not initialized")
		}

		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}

func (s *Server) handleRepoConnect(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if s.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.ConnectRepositoryRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	if req.APIServer != nil {
		if err := s.connectAPIServerAndOpen(ctx, req.APIServer, req.Password, req.ClientOptions); err != nil {
			return nil, err
		}
	} else {
		if err := maybeDecodeToken(&req); err != nil {
			return nil, err
		}

		if err := s.connectAndOpen(ctx, req.Storage, req.Password, req.ClientOptions); err != nil {
			return nil, err
		}
	}

	return s.handleRepoStatus(ctx, r, nil)
}

func (s *Server) handleRepoSetDescription(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req repo.ClientOptions

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	cliOpt := s.rep.ClientOptions()
	cliOpt.Description = req.Description

	if err := repo.SetClientOptions(ctx, s.options.ConfigFile, cliOpt); err != nil {
		return nil, internalServerError(err)
	}

	s.rep.UpdateDescription(req.Description)

	return s.handleRepoStatus(ctx, r, nil)
}

func (s *Server) handleRepoSupportedAlgorithms(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	res := &serverapi.SupportedAlgorithmsResponse{
		DefaultHashAlgorithm: hashing.DefaultAlgorithm,
		HashAlgorithms:       hashing.SupportedAlgorithms(),

		DefaultEncryptionAlgorithm: encryption.DefaultAlgorithm,
		EncryptionAlgorithms:       encryption.SupportedAlgorithms(false),

		DefaultSplitterAlgorithm: splitter.DefaultAlgorithm,
		SplitterAlgorithms:       splitter.SupportedAlgorithms(),
	}

	for k := range compression.ByName {
		res.CompressionAlgorithms = append(res.CompressionAlgorithms, string(k))
	}

	sort.Strings(res.CompressionAlgorithms)

	return res, nil
}

func (s *Server) getConnectOptions(cliOpts repo.ClientOptions) *repo.ConnectOptions {
	o := *s.options.ConnectOptions
	o.ClientOptions = o.ClientOptions.Override(cliOpts)

	return &o
}

func (s *Server) connectAPIServerAndOpen(ctx context.Context, si *repo.APIServerInfo, password string, cliOpts repo.ClientOptions) *apiError {
	if err := repo.ConnectAPIServer(ctx, s.options.ConfigFile, si, password, s.getConnectOptions(cliOpts)); err != nil {
		return repoErrorToAPIError(err)
	}

	return s.open(ctx, password)
}

func (s *Server) connectAndOpen(ctx context.Context, conn blob.ConnectionInfo, password string, cliOpts repo.ClientOptions) *apiError {
	st, err := blob.NewStorage(ctx, conn)
	if err != nil {
		return requestError(serverapi.ErrorStorageConnection, "can't open storage: "+err.Error())
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = repo.Connect(ctx, s.options.ConfigFile, st, password, s.getConnectOptions(cliOpts)); err != nil {
		return repoErrorToAPIError(err)
	}

	return s.open(ctx, password)
}

func (s *Server) open(ctx context.Context, password string) *apiError {
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

func (s *Server) handleRepoDisconnect(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err := s.SetRepository(ctx, nil)
	s.mu.RLock()

	if err != nil {
		return nil, internalServerError(err)
	}

	if err := repo.Disconnect(ctx, s.options.ConfigFile); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleRepoSync(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
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
	switch {
	case errors.Is(err, repo.ErrRepositoryNotInitialized):
		return requestError(serverapi.ErrorNotInitialized, "repository not initialized")
	case errors.Is(err, repo.ErrInvalidPassword):
		return requestError(serverapi.ErrorInvalidPassword, "invalid password")
	case errors.Is(err, repo.ErrAlreadyInitialized):
		return requestError(serverapi.ErrorAlreadyInitialized, "repository already initialized")
	default:
		return internalServerError(errors.Wrap(err, "connect error"))
	}
}
