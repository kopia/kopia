package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/remoterepoapi"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

const syncConnectWaitTime = 5 * time.Second

func (s *Server) handleRepoParameters(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepository)
	if !ok {
		return &serverapi.StatusResponse{
			Connected: false,
		}, nil
	}

	rp := &remoterepoapi.Parameters{
		HashFunction:               dr.ContentReader().ContentFormat().Hash,
		HMACSecret:                 dr.ContentReader().ContentFormat().HMACSecret,
		Format:                     dr.ObjectFormat(),
		SupportsContentCompression: dr.ContentReader().SupportsContentCompression(),
	}

	return rp, nil
}

func (s *Server) handleRepoStatus(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if s.rep == nil {
		return &serverapi.StatusResponse{
			Connected:      false,
			InitRepoTaskID: s.initRepositoryTaskID,
		}, nil
	}

	dr, ok := s.rep.(repo.DirectRepository)
	if ok {
		return &serverapi.StatusResponse{
			Connected:                  true,
			ConfigFile:                 dr.ConfigFilename(),
			Hash:                       dr.ContentReader().ContentFormat().Hash,
			Encryption:                 dr.ContentReader().ContentFormat().Encryption,
			MaxPackSize:                dr.ContentReader().ContentFormat().MaxPackSize,
			Splitter:                   dr.ObjectFormat().Splitter,
			Storage:                    dr.BlobReader().ConnectionInfo().Type,
			ClientOptions:              dr.ClientOptions(),
			SupportsContentCompression: dr.ContentReader().SupportsContentCompression(),
		}, nil
	}

	type remoteRepository interface {
		APIServerURL() string
		SupportsContentCompression() bool
	}

	result := &serverapi.StatusResponse{
		Connected:     true,
		ClientOptions: s.rep.ClientOptions(),
	}

	if rr, ok := s.rep.(remoteRepository); ok {
		result.APIServerURL = rr.APIServerURL()
		result.SupportsContentCompression = rr.SupportsContentCompression()
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

	st, err := blob.NewStorage(ctx, req.Storage, true)
	if err != nil {
		return nil, requestError(serverapi.ErrorStorageConnection, "unable to connect to storage: "+err.Error())
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = repo.Initialize(ctx, st, &req.NewRepositoryOptions, req.Password); err != nil {
		return nil, repoErrorToAPIError(err)
	}

	newRepo, err := s.connectAndOpen(ctx, req.Storage, req.Password, req.ClientOptions)
	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	// release shared lock so that SetRepository can acquire exclusive lock
	s.mu.RUnlock()
	err = s.SetRepository(ctx, newRepo)
	s.mu.RLock()

	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	if err := repo.WriteSession(ctx, s.rep, repo.WriteSessionOptions{
		Purpose: "handleRepoCreate",
	}, func(ctx context.Context, w repo.RepositoryWriter) error {
		if err := policy.SetPolicy(ctx, w, policy.GlobalPolicySourceInfo, policy.DefaultPolicy); err != nil {
			return errors.Wrap(err, "set global policy")
		}

		p := maintenance.DefaultParams()
		p.Owner = w.ClientOptions().UsernameAtHost()

		if err := maintenance.SetParams(ctx, w, &p); err != nil {
			return errors.Wrap(err, "unable to set maintenance params")
		}

		return nil
	}); err != nil {
		return nil, internalServerError(err)
	}

	return s.handleRepoStatus(ctx, r, nil)
}

func (s *Server) handleRepoExists(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	var req serverapi.CheckRepositoryExistsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	st, err := blob.NewStorage(ctx, req.Storage, false)
	if err != nil {
		return nil, internalServerError(err)
	}

	defer st.Close(ctx) // nolint:errcheck

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := st.GetBlob(ctx, repo.FormatBlobID, 0, -1, &tmp); err != nil {
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

	if err := maybeDecodeToken(&req); err != nil {
		return nil, err
	}

	asyncTaskID, err := s.InitRepositoryAsync(ctx, "Connect", func(ctx context.Context) (repo.Repository, error) {
		if req.APIServer != nil {
			return s.connectAPIServerAndOpen(ctx, req.APIServer, req.Password, req.ClientOptions)
		}

		return s.connectAndOpen(ctx, req.Storage, req.Password, req.ClientOptions)
	}, false)
	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	wt := syncConnectWaitTime
	if sec := req.SyncWaitTimeSeconds; sec != 0 {
		wt = time.Second * time.Duration(sec)
	}

	if ti, ok := s.taskmgr.WaitForTask(ctx, asyncTaskID, wt); ok {
		if ti.Error != nil {
			// task has already finished synchronously and failed.
			return nil, repoErrorToAPIError(ti.Error)
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

func (s *Server) handleRepoGetThrottle(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepository)
	if !ok {
		return nil, requestError(serverapi.ErrorStorageConnection, "no direct storage connection")
	}

	return dr.Throttler().Limits(), nil
}

func (s *Server) handleRepoSetThrottle(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	dr, ok := s.rep.(repo.DirectRepository)
	if !ok {
		return nil, requestError(serverapi.ErrorStorageConnection, "no direct storage connection")
	}

	var req throttling.Limits
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to decode request: "+err.Error())
	}

	if err := dr.Throttler().SetLimits(req); err != nil {
		return nil, requestError(serverapi.ErrorMalformedRequest, "unable to set limits: "+err.Error())
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) getConnectOptions(cliOpts repo.ClientOptions) *repo.ConnectOptions {
	o := *s.options.ConnectOptions
	o.ClientOptions = o.ClientOptions.Override(cliOpts)

	return &o
}

func (s *Server) connectAPIServerAndOpen(ctx context.Context, si *repo.APIServerInfo, password string, cliOpts repo.ClientOptions) (repo.Repository, error) {
	if err := passwordpersist.OnSuccess(
		ctx, repo.ConnectAPIServer(ctx, s.options.ConfigFile, si, password, s.getConnectOptions(cliOpts)),
		s.options.PasswordPersist, s.options.ConfigFile, password); err != nil {
		return nil, errors.Wrap(err, "error connecting to API server")
	}

	return s.open(ctx, password)
}

func (s *Server) connectAndOpen(ctx context.Context, conn blob.ConnectionInfo, password string, cliOpts repo.ClientOptions) (repo.Repository, error) {
	st, err := blob.NewStorage(ctx, conn, false)
	if err != nil {
		return nil, errors.Wrap(err, "can't open storage")
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = passwordpersist.OnSuccess(
		ctx, repo.Connect(ctx, s.options.ConfigFile, st, password, s.getConnectOptions(cliOpts)),
		s.options.PasswordPersist, s.options.ConfigFile, password); err != nil {
		return nil, errors.Wrap(err, "error connecting")
	}

	return s.open(ctx, password)
}

func (s *Server) open(ctx context.Context, password string) (repo.Repository, error) {
	// nolint:wrapcheck
	return repo.Open(ctx, s.options.ConfigFile, password, nil)
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

	if err := s.options.PasswordPersist.DeletePassword(ctx, s.options.ConfigFile); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) handleRepoSync(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	if err := s.internalRefreshRLocked(ctx); err != nil {
		return nil, internalServerError(errors.Wrap(err, "unable to refresh repository"))
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
