package server

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/ecc"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

const syncConnectWaitTime = 5 * time.Second

func handleRepoStatus(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	if rc.rep == nil {
		return &serverapi.StatusResponse{
			Connected:      false,
			InitRepoTaskID: rc.srv.getInitRepositoryTaskID(),
		}, nil
	}

	dr, ok := rc.rep.(repo.DirectRepository)
	if ok {
		contentFormat := dr.ContentReader().ContentFormat()

		// this gets potentially stale parameters
		mp := contentFormat.GetCachedMutableParameters()

		return &serverapi.StatusResponse{
			Connected:                  true,
			ConfigFile:                 dr.ConfigFilename(),
			FormatVersion:              mp.Version,
			Hash:                       contentFormat.GetHashFunction(),
			Encryption:                 contentFormat.GetEncryptionAlgorithm(),
			ECC:                        contentFormat.GetECCAlgorithm(),
			ECCOverheadPercent:         contentFormat.GetECCOverheadPercent(),
			MaxPackSize:                mp.MaxPackSize,
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
		ClientOptions: rc.rep.ClientOptions(),
	}

	if rr, ok := rc.rep.(remoteRepository); ok {
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

func handleRepoCreate(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	if rc.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.CreateRepositoryRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
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

	newRepo, err := connectAndOpen(ctx, req.Storage, req.Password, rc.srv.getConnectOptions(req.ClientOptions), rc.srv.getOptions())
	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	err = rc.srv.SetRepository(ctx, newRepo)
	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	if err := repo.WriteSession(ctx, newRepo, repo.WriteSessionOptions{
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

	return handleRepoStatus(ctx, rc)
}

func handleRepoExists(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req serverapi.CheckRepositoryExistsRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	st, err := blob.NewStorage(ctx, req.Storage, false)
	if err != nil {
		return nil, internalServerError(err)
	}

	defer st.Close(ctx) //nolint:errcheck

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := st.GetBlob(ctx, format.KopiaRepositoryBlobID, 0, -1, &tmp); err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil, requestError(serverapi.ErrorNotInitialized, "repository not initialized")
		}

		return nil, internalServerError(err)
	}

	return serverapi.Empty{}, nil
}

func handleRepoConnect(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	if rc.rep != nil {
		return nil, requestError(serverapi.ErrorAlreadyConnected, "already connected")
	}

	var req serverapi.ConnectRepositoryRequest

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	if err := maybeDecodeToken(&req); err != nil {
		return nil, err
	}

	connOpts := rc.srv.getConnectOptions(req.ClientOptions)
	opts := rc.srv.getOptions()

	asyncTaskID, err := rc.srv.InitRepositoryAsync(ctx, "Connect",
		func(ctx context.Context) (repo.Repository, error) {
			if req.APIServer != nil {
				return connectAPIServerAndOpen(ctx, req.APIServer, req.Password, connOpts, opts)
			}

			return connectAndOpen(ctx, req.Storage, req.Password, connOpts, opts)
		}, false)
	if err != nil {
		return nil, repoErrorToAPIError(err)
	}

	wt := syncConnectWaitTime
	if sec := req.SyncWaitTimeSeconds; sec != 0 {
		wt = time.Second * time.Duration(sec)
	}

	if ti, ok := rc.srv.taskManager().WaitForTask(ctx, asyncTaskID, wt); ok {
		if ti.Error != nil {
			// task has already finished synchronously and failed.
			return nil, repoErrorToAPIError(ti.Error)
		}
	}

	return handleRepoStatus(ctx, rc)
}

func handleRepoSetDescription(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	var req repo.ClientOptions

	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
	}

	cliOpt := rc.rep.ClientOptions()
	cliOpt.Description = req.Description

	if err := repo.SetClientOptions(ctx, rc.srv.getOptions().ConfigFile, cliOpt); err != nil {
		return nil, internalServerError(err)
	}

	rc.rep.UpdateDescription(req.Description)

	return handleRepoStatus(ctx, rc)
}

func handleRepoSupportedAlgorithms(ctx context.Context, _ requestContext) (interface{}, *apiError) {
	res := &serverapi.SupportedAlgorithmsResponse{
		DefaultHashAlgorithm:    hashing.DefaultAlgorithm,
		SupportedHashAlgorithms: toAlgorithmInfo(hashing.SupportedAlgorithms(), neverDeprecated),

		DefaultEncryptionAlgorithm:    encryption.DefaultAlgorithm,
		SupportedEncryptionAlgorithms: toAlgorithmInfo(encryption.SupportedAlgorithms(false), neverDeprecated),

		DefaultECCAlgorithm:    ecc.DefaultAlgorithm,
		SupportedECCAlgorithms: toAlgorithmInfo(ecc.SupportedAlgorithms(), neverDeprecated),

		DefaultSplitterAlgorithm:    splitter.DefaultAlgorithm,
		SupportedSplitterAlgorithms: toAlgorithmInfo(splitter.SupportedAlgorithms(), neverDeprecated),
	}

	for k := range compression.ByName {
		res.SupportedCompressionAlgorithms = append(res.SupportedCompressionAlgorithms, serverapi.AlgorithmInfo{
			ID:         string(k),
			Deprecated: compression.IsDeprecated[k],
		})
	}

	sortAlgorithms(res.SupportedHashAlgorithms)
	sortAlgorithms(res.SupportedEncryptionAlgorithms)
	sortAlgorithms(res.SupportedECCAlgorithms)
	sortAlgorithms(res.SupportedCompressionAlgorithms)
	sortAlgorithms(res.SupportedSplitterAlgorithms)

	return res, nil
}

func neverDeprecated(string) bool {
	return false
}

func toAlgorithmInfo(names []string, isDeprecated func(id string) bool) []serverapi.AlgorithmInfo {
	var result []serverapi.AlgorithmInfo

	for _, n := range names {
		result = append(result, serverapi.AlgorithmInfo{
			ID:         n,
			Deprecated: isDeprecated(n),
		})
	}

	return result
}

func sortAlgorithms(a []serverapi.AlgorithmInfo) {
	sort.Slice(a, func(i, j int) bool {
		if l, r := a[i].Deprecated, a[j].Deprecated; l != r {
			// non-deprecated first
			return !l
		}

		return a[i].ID < a[j].ID
	})
}

func handleRepoGetThrottle(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	dr, ok := rc.rep.(repo.DirectRepository)
	if !ok {
		return nil, requestError(serverapi.ErrorStorageConnection, "no direct storage connection")
	}

	return dr.Throttler().Limits(), nil
}

func handleRepoSetThrottle(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	dr, ok := rc.rep.(repo.DirectRepository)
	if !ok {
		return nil, requestError(serverapi.ErrorStorageConnection, "no direct storage connection")
	}

	var req throttling.Limits
	if err := json.Unmarshal(rc.body, &req); err != nil {
		return nil, unableToDecodeRequest(err)
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

func connectAPIServerAndOpen(ctx context.Context, si *repo.APIServerInfo, password string, connectOpts *repo.ConnectOptions, opts *Options) (repo.Repository, error) {
	if err := passwordpersist.OnSuccess(
		ctx, repo.ConnectAPIServer(ctx, opts.ConfigFile, si, password, connectOpts),
		opts.PasswordPersist, opts.ConfigFile, password); err != nil {
		return nil, errors.Wrap(err, "error connecting to API server")
	}

	//nolint:wrapcheck
	return repo.Open(ctx, opts.ConfigFile, password, nil)
}

func connectAndOpen(ctx context.Context, conn blob.ConnectionInfo, password string, connectOpts *repo.ConnectOptions, opts *Options) (repo.Repository, error) {
	st, err := blob.NewStorage(ctx, conn, false)
	if err != nil {
		return nil, errors.Wrap(err, "can't open storage")
	}
	defer st.Close(ctx) //nolint:errcheck

	if err = passwordpersist.OnSuccess(
		ctx, repo.Connect(ctx, opts.ConfigFile, st, password, connectOpts),
		opts.PasswordPersist, opts.ConfigFile, password); err != nil {
		return nil, errors.Wrap(err, "error connecting")
	}

	//nolint:wrapcheck
	return repo.Open(ctx, opts.ConfigFile, password, nil)
}

func handleRepoDisconnect(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	if err := rc.srv.disconnect(ctx); err != nil {
		return nil, internalServerError(err)
	}

	return &serverapi.Empty{}, nil
}

func (s *Server) disconnect(ctx context.Context) error {
	if err := s.SetRepository(ctx, nil); err != nil {
		return err
	}

	if err := repo.Disconnect(ctx, s.options.ConfigFile); err != nil {
		//nolint:wrapcheck
		return err
	}

	if err := s.options.PasswordPersist.DeletePassword(ctx, s.options.ConfigFile); err != nil {
		//nolint:wrapcheck
		return err
	}

	return nil
}

func handleRepoSync(ctx context.Context, rc requestContext) (interface{}, *apiError) {
	rc.srv.Refresh()

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
